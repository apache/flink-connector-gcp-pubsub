package org.apache.flink.connector.gcp.pubsub.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContextAnyThreadMailbox;
import org.apache.flink.connector.gcp.pubsub.sink.util.TestGcpWriterClient;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.FlinkRuntimeException;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Tests for {@link PubSubWriter}. */
class PubSubWriterTest {

    private static final int MAXIMUM_INFLIGHT_MESSAGES = 3;

    @Test
    void writeMessageDeliversMessageUsingClient() throws IOException, InterruptedException {
        TestGcpWriterClient client = new TestGcpWriterClient();
        PubSubWriter<String> writer =
                getDefaultWriter(client, new TestSinkInitContext(), MAXIMUM_INFLIGHT_MESSAGES);
        String message = "test-message";
        writer.write(message, null);
        client.deliverMessage(message);
        writer.flush(false);

        assertThat(client.getDeliveredMessages()).containsExactly(message);
    }

    @Test
    void writeMessageIncrementsMetricsOnDelivery() throws IOException, InterruptedException {
        TestGcpWriterClient client = new TestGcpWriterClient();
        WriterInitContext context = new TestSinkInitContext();

        PubSubWriter<String> writer = getDefaultWriter(client, context, MAXIMUM_INFLIGHT_MESSAGES);
        String message = "test-message";

        // write message
        writer.write(message, null);

        // get metrics before delivery
        Counter numBytesOutCounter = context.metricGroup().getNumBytesSendCounter();
        Counter numRecordsSendCounter = context.metricGroup().getNumRecordsSendCounter();
        long recordsSentBeforeDelivery = numRecordsSendCounter.getCount();
        long bytesSentBeforeDelivery = numBytesOutCounter.getCount();

        // deliver message
        client.deliverMessage(message);
        writer.flush(false);

        long messageSize =
                PubsubMessage.newBuilder()
                        .setData(ByteString.copyFromUtf8(message))
                        .build()
                        .getSerializedSize();

        assertThat(recordsSentBeforeDelivery).isEqualTo(0);
        assertThat(bytesSentBeforeDelivery).isEqualTo(0);
        assertThat(numRecordsSendCounter.getCount()).isEqualTo(1);
        assertThat(numBytesOutCounter.getCount()).isEqualTo(messageSize);
    }

    @Test
    void writerFlushesPublisherOnFlush() throws IOException, InterruptedException {
        TestGcpWriterClient client = new TestGcpWriterClient();
        PubSubWriter<String> writer =
                getDefaultWriter(client, new TestSinkInitContext(), MAXIMUM_INFLIGHT_MESSAGES);
        writer.write("test-message", null);
        writer.flush(false);
        assertThat(client.isFlushed()).isTrue();
    }

    @Test
    void writeMessageDoesNotBlockBeforeMaximumInflight() throws IOException, InterruptedException {
        TestGcpWriterClient client = new TestGcpWriterClient();

        PubSubWriter<String> writer =
                getDefaultWriter(client, new TestSinkInitContextAnyThreadMailbox(), 2);

        String firstMessage = "first-message";
        String secondMessage = "second-message";
        CountDownLatch finsishedLatch1 = new CountDownLatch(1);
        CountDownLatch hasStarted1 = new CountDownLatch(1);
        CountDownLatch finsishedLatch2 = new CountDownLatch(1);
        CountDownLatch hasStarted2 = new CountDownLatch(1);

        writeMessageToWriterAsync(writer, firstMessage, hasStarted1, finsishedLatch1);
        writeMessageToWriterAsync(writer, secondMessage, hasStarted2, finsishedLatch2);
        Boolean firstMessageAttemptSent =
                hasStarted1.await(1, java.util.concurrent.TimeUnit.SECONDS);
        Boolean secondMessageAttemptSent =
                hasStarted2.await(1, java.util.concurrent.TimeUnit.SECONDS);
        Boolean firstMessageSent = finsishedLatch1.await(1, java.util.concurrent.TimeUnit.SECONDS);
        Boolean secondMessageSent = finsishedLatch2.await(1, java.util.concurrent.TimeUnit.SECONDS);

        client.deliverMessage(firstMessage);
        client.deliverMessage(secondMessage);
        writer.flush(false);

        assertThat(firstMessageAttemptSent).isTrue();
        assertThat(secondMessageAttemptSent).isTrue();
        assertThat(firstMessageSent).isTrue();
        assertThat(secondMessageSent).isTrue();
        assertThat(client.getDeliveredMessages())
                .containsExactlyInAnyOrder(firstMessage, secondMessage);
    }

    @Test
    void writeMessageBlocksAfterMaximumInflight() throws IOException, InterruptedException {
        TestGcpWriterClient client = new TestGcpWriterClient();
        PubSubWriter<String> writer =
                getDefaultWriter(client, new TestSinkInitContextAnyThreadMailbox(), 1);

        String firstMessage = "first-message";
        String secondMessage = "second-message";
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch hasStarted1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        CountDownLatch hasStarted2 = new CountDownLatch(1);

        writeMessageToWriterAsync(writer, firstMessage, hasStarted1, latch1);
        Boolean firstMessageAttemptSent =
                hasStarted1.await(1, java.util.concurrent.TimeUnit.SECONDS);
        Boolean firstMessageSent = latch1.await(1, java.util.concurrent.TimeUnit.SECONDS);

        writeMessageToWriterAsync(writer, secondMessage, hasStarted2, latch2);
        Boolean secondMessageAttemptSent =
                hasStarted2.await(1, java.util.concurrent.TimeUnit.SECONDS);
        Boolean secondMessageBlocked = !latch2.await(1, java.util.concurrent.TimeUnit.SECONDS);

        client.deliverMessage(firstMessage);

        Boolean secondMessageSent = latch2.await(1, java.util.concurrent.TimeUnit.SECONDS);

        client.deliverMessage(secondMessage);
        writer.flush(false);

        assertThat(firstMessageAttemptSent).isTrue();
        assertThat(secondMessageAttemptSent).isTrue();
        assertThat(firstMessageSent).isTrue();
        assertThat(secondMessageBlocked).isTrue();
        assertThat(secondMessageSent).isTrue();
        assertThat(client.getDeliveredMessages()).containsExactly(firstMessage, secondMessage);
    }

    @Test
    void writerRetriesOnFailureIfFailOnErrorIsUnset() throws IOException, InterruptedException {
        TestGcpWriterClient client = new TestGcpWriterClient();
        client.setExceptionSupplier(message -> new StatusRuntimeException(io.grpc.Status.INTERNAL));

        PubSubWriter<String> writer =
                getDefaultWriter(client, new TestSinkInitContext(), MAXIMUM_INFLIGHT_MESSAGES);
        String message = "fail-message";
        writer.write(message, null);
        client.deliverMessage(message);
        writer.flush(false);

        assertThat(client.failedInitially(message)).isTrue();
        assertThat(client.getDeliveredMessages()).containsExactly(message);
    }

    @Test
    void writerFailsOnErrorIfFailOnErrorIsSet() throws IOException, InterruptedException {
        TestGcpWriterClient client = new TestGcpWriterClient();
        client.setExceptionSupplier(message -> new StatusRuntimeException(io.grpc.Status.INTERNAL));
        PubSubWriter<String> writer =
                new PubSubWriter<>(
                        client,
                        new TestSinkInitContext(),
                        new SimpleStringSchema(),
                        MAXIMUM_INFLIGHT_MESSAGES,
                        true);
        String message = "fail-message";
        writer.write(message, null);

        assertThatExceptionOfType(FlinkRuntimeException.class)
                .isThrownBy(() -> writer.flush(false))
                .withMessage("Failed to publish message to PubSub")
                .withCauseInstanceOf(StatusRuntimeException.class);
    }

    @Test
    void writerFailsOnProjectNotFoundExceptionWithFailOnErrorFalse()
            throws IOException, InterruptedException {
        TestGcpWriterClient client = new TestGcpWriterClient();
        client.setExceptionSupplier(
                message -> new StatusRuntimeException(io.grpc.Status.NOT_FOUND));
        PubSubWriter<String> writer =
                new PubSubWriter<>(
                        client,
                        new TestSinkInitContext(),
                        new SimpleStringSchema(),
                        MAXIMUM_INFLIGHT_MESSAGES,
                        false);
        String message = "fail-message";
        writer.write(message, null);

        assertThatExceptionOfType(PubSubException.class)
                .isThrownBy(() -> writer.flush(false))
                .withMessage("Topic not found")
                .withCauseInstanceOf(StatusRuntimeException.class);
    }

    @Test
    void closeWriterClosesChannelAndConnection() throws Exception {
        TestGcpWriterClient client = new TestGcpWriterClient();
        PubSubWriter<String> writer =
                getDefaultWriter(client, new TestSinkInitContext(), MAXIMUM_INFLIGHT_MESSAGES);
        writer.close();
        assertThat(client.isShutdown()).isTrue();
    }

    private void writeMessageToWriterAsync(
            PubSubWriter<String> writer,
            String message,
            CountDownLatch hasStarted,
            CountDownLatch finishedLatch) {
        Thread thread =
                new Thread(
                        () -> {
                            hasStarted.countDown();
                            try {
                                writer.write(message, null);
                            } catch (IOException | InterruptedException ignored) {
                            }
                            finishedLatch.countDown();
                        });
        thread.start();
    }

    private PubSubWriter<String> getDefaultWriter(
            TestGcpWriterClient client, WriterInitContext context, int maximumInflightRequests) {
        return new PubSubWriter<>(
                client, context, new SimpleStringSchema(), maximumInflightRequests, false);
    }
}
