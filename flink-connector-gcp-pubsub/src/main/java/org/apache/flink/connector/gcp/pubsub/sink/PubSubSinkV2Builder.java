package org.apache.flink.connector.gcp.pubsub.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.gcp.pubsub.sink.config.GcpPublisherConfig;

import java.util.Optional;

/**
 * A builder for creating a {@link PubSubSinkV2}.
 *
 * <p>The builder uses the following parameters to build a {@link PubSubSinkV2}:
 *
 * <ul>
 *   <li>{@link GcpPublisherConfig} for the {@link com.google.cloud.pubsub.v1.Publisher}
 *       configuration.
 *   <li>{@link SerializationSchema} for serializing the input data.
 *   <li>{@code projectId} for the name of the project where the topic is located.
 *   <li>{@code topicId} for the name of the topic to send messages to.
 *   <li>{@code maximumInflightMessages} for the maximum number of inflight messages.
 *   <li>{@code failOnError} for whether to fail on an error.
 * </ul>
 *
 * <p>It can be used as follows:
 *
 * <pre>{@code
 * PubSubSinkV2Builder<String> pubSubSink = {@code PubSubSinkV2Builder}.<String>builder()
 *     .setProjectId("project-id")
 *     .setTopicId("topic-id)
 *     .setGcpPublisherConfig(gcpPublisherConfig)
 *     .setSerializationSchema(new SimpleStringSchema())
 *     .setMaximumInflightMessages(10)
 *     .setFailOnError(true)
 *     .build();
 *
 * }</pre>
 *
 * @param <T>
 */
@PublicEvolving
public class PubSubSinkV2Builder<T> {

    public static final int DEFAULT_MAXIMUM_INFLIGHT_REQUESTS = 1000;

    public static final boolean DEFAULT_FAIL_ON_ERROR = false;
    private String projectId;
    private String topicId;
    private SerializationSchema<T> serializationSchema;
    private GcpPublisherConfig gcpPublisherConfig;
    private Integer numMaxInflightRequests;
    private Boolean failOnError;

    public PubSubSinkV2Builder<T> setProjectId(String projectId) {
        this.projectId = projectId;
        return this;
    }

    public PubSubSinkV2Builder<T> setTopicId(String topicId) {
        this.topicId = topicId;
        return this;
    }

    public PubSubSinkV2Builder<T> setSerializationSchema(
            SerializationSchema<T> serializationSchema) {
        this.serializationSchema = serializationSchema;
        return this;
    }

    public PubSubSinkV2Builder<T> setGcpPublisherConfig(GcpPublisherConfig gcpPublisherConfig) {
        this.gcpPublisherConfig = gcpPublisherConfig;
        return this;
    }

    public PubSubSinkV2Builder<T> setNumMaxInflightRequests(int numMaxInflightRequests) {
        this.numMaxInflightRequests = numMaxInflightRequests;
        return this;
    }

    public PubSubSinkV2Builder<T> setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    public PubSubSinkV2<T> build() {
        return new PubSubSinkV2<>(
                projectId,
                topicId,
                serializationSchema,
                gcpPublisherConfig,
                Optional.ofNullable(numMaxInflightRequests)
                        .orElse(DEFAULT_MAXIMUM_INFLIGHT_REQUESTS),
                Optional.ofNullable(failOnError).orElse(DEFAULT_FAIL_ON_ERROR));
    }
}
