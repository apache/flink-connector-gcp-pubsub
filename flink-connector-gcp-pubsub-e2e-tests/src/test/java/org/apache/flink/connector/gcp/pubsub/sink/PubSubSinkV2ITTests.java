package org.apache.flink.connector.gcp.pubsub.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.functions.FromElementsGeneratorFunction;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.gcp.pubsub.sink.config.GcpPublisherConfig;
import org.apache.flink.connector.gcp.pubsub.sink.util.PubsubHelper;
import org.apache.flink.connector.gcp.pubsub.sink.util.TestChannelProvider;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.EmulatorCredentialsProvider;
import org.apache.flink.streaming.connectors.gcp.pubsub.test.DockerImageVersions;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.ExceptionUtils;

import com.google.api.gax.retrying.RetrySettings;
import com.google.pubsub.v1.ReceivedMessage;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link PubSubSinkV2} using {@link PubSubEmulatorContainer}. */
@ExtendWith(MiniClusterExtension.class)
@Execution(ExecutionMode.CONCURRENT)
@Testcontainers
class PubSubSinkV2ITTests {

    private static final String PROJECT_ID = "test-project";

    private static final String TOPIC_ID = "test-topic";

    private static final String SUBSCRIPTION_ID = "test-subscription";

    private StreamExecutionEnvironment env;

    @Container
    private static final PubSubEmulatorContainer PUB_SUB_EMULATOR_CONTAINER =
            new PubSubEmulatorContainer(
                    DockerImageName.parse(DockerImageVersions.GOOGLE_CLOUD_PUBSUB_EMULATOR));

    private PubsubHelper pubSubHelper;

    @BeforeEach
    void setUp() throws IOException {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        pubSubHelper = new PubsubHelper(PUB_SUB_EMULATOR_CONTAINER.getEmulatorEndpoint());

        pubSubHelper.createTopic(PROJECT_ID, TOPIC_ID);
        pubSubHelper.createSubscription(PROJECT_ID, SUBSCRIPTION_ID, PROJECT_ID, TOPIC_ID);
    }

    @AfterEach
    void tearDown() throws IOException {
        pubSubHelper.deleteSubscription(PROJECT_ID, SUBSCRIPTION_ID);
        pubSubHelper.deleteTopic(PROJECT_ID, TOPIC_ID);
        pubSubHelper.close();
    }

    @Test
    void pubSubSinkV2DeliversRecords() throws Exception {
        String[] elements = new String[] {"test1", "test2", "test3"};
        DataStreamSource<String> stream =
                env.fromSource(
                        new DataGeneratorSource<>(
                                new FromElementsGeneratorFunction<>(
                                        BasicTypeInfo.STRING_TYPE_INFO, elements),
                                elements.length,
                                TypeInformation.of(String.class)),
                        WatermarkStrategy.noWatermarks(),
                        "DataGeneratorSource");

        GcpPublisherConfig gcpPublisherConfig =
                GcpPublisherConfig.builder()
                        .setCredentialsProvider(EmulatorCredentialsProvider.create())
                        .setTransportChannelProvider(
                                new TestChannelProvider(
                                        PUB_SUB_EMULATOR_CONTAINER.getEmulatorEndpoint()))
                        .build();

        PubSubSinkV2<String> sink =
                PubSubSinkV2.<String>builder()
                        .setProjectId(PROJECT_ID)
                        .setTopicId(TOPIC_ID)
                        .setSerializationSchema(new SimpleStringSchema())
                        .setGcpPublisherConfig(gcpPublisherConfig)
                        .setFailOnError(true)
                        .build();
        stream.sinkTo(sink);
        int maxNumberOfMessages = elements.length;
        env.execute("PubSubSinkV2ITTests");
        List<ReceivedMessage> receivedMessages =
                pubSubHelper.pullMessages(PROJECT_ID, SUBSCRIPTION_ID, maxNumberOfMessages);

        assertThat(receivedMessages).hasSameSizeAs(elements);
        assertThat(receivedMessages)
                .extracting(ReceivedMessage::getMessage)
                .extracting(message -> message.getData().toStringUtf8())
                .containsExactlyInAnyOrder(elements);
    }

    @Test
    void pubSubSinkV2PropagatesException() throws Exception {
        String[] elements = new String[] {"test1", "test2", "test3"};
        DataStreamSource<String> stream =
                env.fromSource(
                        new DataGeneratorSource<>(
                                new FromElementsGeneratorFunction<>(
                                        BasicTypeInfo.STRING_TYPE_INFO, elements),
                                elements.length,
                                TypeInformation.of(String.class)),
                        WatermarkStrategy.noWatermarks(),
                        "DataGeneratorSource");

        GcpPublisherConfig gcpPublisherConfig =
                GcpPublisherConfig.builder()
                        .setCredentialsProvider(EmulatorCredentialsProvider.create())
                        .setTransportChannelProvider(new TestChannelProvider("bad-endpoint:1234"))
                        .setRetrySettings(
                                RetrySettings.newBuilder()
                                        .setMaxAttempts(1)
                                        .setTotalTimeout(Duration.ofSeconds(10))
                                        .setInitialRetryDelay(Duration.ofMillis(100))
                                        .setRetryDelayMultiplier(1.3)
                                        .setMaxRetryDelay(Duration.ofSeconds(5))
                                        .setInitialRpcTimeout(Duration.ofSeconds(5))
                                        .setRpcTimeoutMultiplier(1)
                                        .setMaxRpcTimeout(Duration.ofSeconds(10))
                                        .build())
                        .build();

        PubSubSinkV2<String> sink =
                PubSubSinkV2.<String>builder()
                        .setProjectId(PROJECT_ID)
                        .setTopicId(TOPIC_ID)
                        .setSerializationSchema(new SimpleStringSchema())
                        .setGcpPublisherConfig(gcpPublisherConfig)
                        .setFailOnError(true)
                        .build();
        stream.sinkTo(sink);
        Assertions.assertThatExceptionOfType(JobExecutionException.class)
                .isThrownBy(() -> env.execute("PubSubSinkV2ITTests"))
                .satisfies(
                        e ->
                                ExceptionUtils.findThrowable(e, UnknownHostException.class)
                                        .isPresent());
    }
}
