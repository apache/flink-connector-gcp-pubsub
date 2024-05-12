package org.apache.flink.connector.gcp.pubsub.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.gcp.pubsub.sink.config.GcpPublisherConfig;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.concurrent.Executors.directExecutor;

/**
 * A stateless {@link SinkWriter} that writes records to PubSub using generic {@link
 * GcpWriterClient}. The writer blocks on completion of inflight requests on {@code flush()} and
 * {@code close()}. The writer also uses {@code maxInFlightRequests} and blocks new writes if the
 * number of inflight requests exceeds the specified limit.
 *
 * @param <T> The type of the records .
 */
@Internal
public class PubSubWriter<T> implements SinkWriter<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubWriter.class);

    /** The PubSub generic client to publish messages. */
    private final GcpWriterClient<PubsubMessage> publisher;

    /**
     * The maximum number of inflight requests, The writer blocks new writes if the number of
     * inflight requests exceeds the specified limit.
     */
    private final int maximumInflightRequests;

    /**
     * Flag to indicate whether to fail on errors, if unset the writer will retry non-fatal request
     * failures.
     */
    private final boolean failOnError;

    private int inflightRequests = 0;

    private final MailboxExecutor mailboxExecutor;
    private final Counter numBytesOutCounter;
    private final Counter numRecordsOutCounter;
    private final Counter numRecordsOutErrorCounter;
    private final SerializationSchema<T> serializationSchema;

    PubSubWriter(
            String projectId,
            String topicId,
            SerializationSchema<T> serializationSchema,
            WriterInitContext context,
            GcpPublisherConfig publisherConfig,
            int maximumInflightRequests,
            boolean failOnError)
            throws IOException {
        this(
                createPublisher(projectId, topicId, publisherConfig),
                context,
                serializationSchema,
                maximumInflightRequests,
                failOnError);
    }

    @VisibleForTesting
    PubSubWriter(
            GcpWriterClient<PubsubMessage> publisher,
            WriterInitContext context,
            SerializationSchema<T> serializationSchema,
            int maximumInflightRequests,
            boolean failOnError) {
        this.publisher = Preconditions.checkNotNull(publisher);
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
        Preconditions.checkNotNull(context, "Context cannot be null.");

        this.mailboxExecutor = context.getMailboxExecutor();
        this.numBytesOutCounter = context.metricGroup().getIOMetricGroup().getNumBytesOutCounter();
        this.numRecordsOutCounter =
                context.metricGroup().getIOMetricGroup().getNumRecordsOutCounter();
        this.numRecordsOutErrorCounter = context.metricGroup().getNumRecordsOutErrorsCounter();
        this.maximumInflightRequests = maximumInflightRequests;
        this.failOnError = failOnError;
    }

    @Override
    public void write(T t, SinkWriter.Context context) throws IOException, InterruptedException {
        awaitMaxInflightRequestsBelow(maximumInflightRequests);
        PubsubMessage message =
                PubsubMessage.newBuilder()
                        .setData(ByteString.copyFrom(serializationSchema.serialize(t)))
                        .build();
        publishMessage(message);
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {
        publisher.flush();
        awaitMaxInflightRequestsBelow(1);
    }

    private void awaitMaxInflightRequestsBelow(long maxInflightRequests)
            throws InterruptedException {
        /* Block until inflight callbacks are executed. We use this mechanism to introduce
        backpressure when inflight requests exceed the maximum allowed and to ensure that
        snapshot barriers are not sent before inflight requests are completed. */
        while (inflightRequests >= maxInflightRequests) {
            mailboxExecutor.yield();
        }
    }

    private void publishMessage(PubsubMessage message) {
        ApiFuture<String> future = publisher.publish(message);
        inflightRequests++;
        LOG.debug("Publishing message with id {}", message.getMessageId());
        ApiFutures.addCallback(
                future,
                new ApiFutureCallback<String>() {
                    @Override
                    public void onFailure(Throwable throwable) {
                        mailboxExecutor.execute(
                                () -> failMessage(message, throwable),
                                "Handling failure of PubSub message publishing.");
                    }

                    @Override
                    public void onSuccess(String ignored) {
                        mailboxExecutor.execute(
                                () -> completeMessage(message),
                                "Handling success of PubSub message publishing.");
                    }
                },
                directExecutor());
    }

    private void completeMessage(PubsubMessage message) {
        LOG.debug("Delivered message with id {}", message.getMessageId());
        inflightRequests--;
        numBytesOutCounter.inc(message.getSerializedSize());
        numRecordsOutCounter.inc();
    }

    private void failMessage(PubsubMessage message, Throwable throwable) {
        LOG.warn(
                String.format(
                        "Failed to publish message with id %s to PubSub", message.getMessageId()),
                throwable);
        inflightRequests--;
        numRecordsOutErrorCounter.inc();
        raiseIfFatal(throwable);

        if (failOnError) {
            throw new FlinkRuntimeException("Failed to publish message to PubSub", throwable);
        }

        publishMessage(message);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing PubSub writer.");
        publisher.shutdown();
    }

    private void raiseIfFatal(Throwable throwable) {
        PubSubExceptionClassifiers.getFatalExceptionClassifier()
                .isFatal(
                        throwable,
                        e ->
                                mailboxExecutor.execute(
                                        () -> {
                                            throw e;
                                        },
                                        "Raise fatal exception."));
    }

    private static PubSubWriterClient createPublisher(
            String projectId, String topicId, GcpPublisherConfig publisherConfig)
            throws IOException {
        Publisher.Builder builder = Publisher.newBuilder(TopicName.of(projectId, topicId));
        builder.setCredentialsProvider(publisherConfig.getCredentialsProvider());
        Optional.ofNullable(publisherConfig.getBatchingSettings())
                .ifPresent(builder::setBatchingSettings);
        Optional.ofNullable(publisherConfig.getRetrySettings())
                .ifPresent(builder::setRetrySettings);
        Optional.ofNullable(publisherConfig.getTransportChannelProvider())
                .ifPresent(builder::setChannelProvider);
        Optional.ofNullable(publisherConfig.getEnableCompression())
                .ifPresent(builder::setEnableCompression);

        return PubSubWriterClient.of(builder.build());
    }
}
