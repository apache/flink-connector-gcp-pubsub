package org.apache.flink.connector.gcp.pubsub.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.gcp.pubsub.sink.config.GcpPublisherConfig;

import com.google.api.gax.core.NoCredentialsProvider;
import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.gcp.pubsub.sink.PubSubSinkV2Builder.DEFAULT_FAIL_ON_ERROR;
import static org.apache.flink.connector.gcp.pubsub.sink.PubSubSinkV2Builder.DEFAULT_MAXIMUM_INFLIGHT_REQUESTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Tests for {@link PubSubSinkV2Builder}. */
class PubSubSinkV2BuilderTest {

    @Test
    void builderBuildsSinkWithCorrectProperties() {
        PubSubSinkV2Builder<String> builder = PubSubSinkV2.<String>builder();
        GcpPublisherConfig gcpPublisherConfig =
                GcpPublisherConfig.builder()
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .build();

        SerializationSchema<String> serializationSchema = new SimpleStringSchema();

        builder.setProjectId("test-project-id")
                .setTopicId("test-topic-id")
                .setGcpPublisherConfig(gcpPublisherConfig)
                .setSerializationSchema(serializationSchema)
                .setNumMaxInflightRequests(10)
                .setFailOnError(true);
        PubSubSinkV2<String> sink = builder.build();

        assertThat(sink).hasFieldOrPropertyWithValue("projectId", "test-project-id");
        assertThat(sink).hasFieldOrPropertyWithValue("topicId", "test-topic-id");
        assertThat(sink).hasFieldOrPropertyWithValue("serializationSchema", serializationSchema);
        assertThat(sink).hasFieldOrPropertyWithValue("publisherConfig", gcpPublisherConfig);
        assertThat(sink).hasFieldOrPropertyWithValue("maxInFlightRequests", 10);
        assertThat(sink).hasFieldOrPropertyWithValue("failOnError", true);
    }

    @Test
    void builderBuildsSinkWithDefaultProperties() {
        PubSubSinkV2Builder<String> builder = PubSubSinkV2.<String>builder();
        SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        GcpPublisherConfig gcpPublisherConfig =
                GcpPublisherConfig.builder()
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .build();

        builder.setProjectId("test-project-id")
                .setTopicId("test-topic-id")
                .setGcpPublisherConfig(gcpPublisherConfig)
                .setSerializationSchema(serializationSchema);
        PubSubSinkV2<String> sink = builder.build();

        assertThat(sink)
                .hasFieldOrPropertyWithValue(
                        "maxInFlightRequests", DEFAULT_MAXIMUM_INFLIGHT_REQUESTS);
        assertThat(sink).hasFieldOrPropertyWithValue("failOnError", DEFAULT_FAIL_ON_ERROR);
    }

    @Test
    void builderThrowsNullPointerExceptionOnNullProjectId() {
        PubSubSinkV2Builder<String> builder = new PubSubSinkV2Builder<>();
        SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        GcpPublisherConfig gcpPublisherConfig =
                GcpPublisherConfig.builder()
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .build();

        builder.setTopicId("test-topic-id")
                .setGcpPublisherConfig(gcpPublisherConfig)
                .setSerializationSchema(serializationSchema);

        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(builder::build)
                .withMessage("Project ID cannot be null.");
    }

    @Test
    void gcpPublisherConfigReturnsNullTransportChannel() {
        PubSubSinkV2Builder<String> builder = new PubSubSinkV2Builder<>();
        SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        GcpPublisherConfig gcpPublisherConfig =
                GcpPublisherConfig.builder()
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .build();

        assertThat(gcpPublisherConfig).hasFieldOrPropertyWithValue("transportChannelProvider", null);
    }

    @Test
    void builderThrowsNullPointerExceptionOnNullTopicId() {
        PubSubSinkV2Builder<String> builder = PubSubSinkV2.<String>builder();
        SerializationSchema<String> serializationSchema = new SimpleStringSchema();
        GcpPublisherConfig gcpPublisherConfig =
                GcpPublisherConfig.builder()
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .build();

        builder.setProjectId("test-project-id")
                .setGcpPublisherConfig(gcpPublisherConfig)
                .setSerializationSchema(serializationSchema);

        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(builder::build)
                .withMessage("Topic ID cannot be null.");
    }

    @Test
    void builderThrowsNullPointerExceptionOnNullGcpPublisherConfig() {
        PubSubSinkV2Builder<String> builder = PubSubSinkV2.<String>builder();
        SerializationSchema<String> serializationSchema = new SimpleStringSchema();

        builder.setProjectId("test-project-id")
                .setTopicId("test-topic-id")
                .setSerializationSchema(serializationSchema);

        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(builder::build)
                .withMessage("Connection configs cannot be null.");
    }

    @Test
    void builderThrowsNullPointerExceptionOnNullSerializationSchema() {
        PubSubSinkV2Builder<String> builder = PubSubSinkV2.<String>builder();
        GcpPublisherConfig gcpPublisherConfig =
                GcpPublisherConfig.builder()
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .build();

        builder.setProjectId("test-project-id")
                .setTopicId("test-topic-id")
                .setGcpPublisherConfig(gcpPublisherConfig);

        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(builder::build)
                .withMessage("Serialization schema cannot be null.");
    }

    @Test
    void builderThrowsIllegalArgumentExceptionWhenUsingNegativeInflightRequests() {
        PubSubSinkV2Builder<String> builder = PubSubSinkV2.<String>builder();
        GcpPublisherConfig gcpPublisherConfig =
                GcpPublisherConfig.builder()
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .build();

        builder.setProjectId("test-project-id")
                .setTopicId("test-topic-id")
                .setGcpPublisherConfig(gcpPublisherConfig)
                .setSerializationSchema(new SimpleStringSchema())
                .setNumMaxInflightRequests(-1);

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(builder::build)
                .withMessage("Max inflight requests must be greater than 0.");
    }
}
