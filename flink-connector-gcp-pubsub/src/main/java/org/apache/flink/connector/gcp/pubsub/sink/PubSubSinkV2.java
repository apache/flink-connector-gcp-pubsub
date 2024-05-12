package org.apache.flink.connector.gcp.pubsub.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.gcp.pubsub.sink.config.GcpPublisherConfig;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A {@link Sink} to produce data into Gcp PubSub. The sink uses the {@link GcpPublisherConfig} to
 * create a PubSub {@link com.google.cloud.pubsub.v1.Publisher} to send messages to the specified
 * topic under a specific project. The sink uses the provided {@link SerializationSchema} to
 * serialize the input elements into PubSub messages.
 *
 * <p>The sink is stateless and blocks on completion of inflight requests on {@code flush()} and
 * {@code close()}. The sink also uses {@code maxInFlightRequests} and blocks new writes if the
 * number of inflight requests exceeds the specified limit.
 *
 * @param <T> input type for the sink.
 */
@PublicEvolving
public class PubSubSinkV2<T> implements Sink<T> {

    private final String projectId;
    private final String topicId;

    /** Serialization schema to serialize input elements into PubSub messages. */
    private final SerializationSchema<T> serializationSchema;

    /** Configuration for the PubSub publisher. */
    private final GcpPublisherConfig publisherConfig;

    private final int maxInFlightRequests;

    private final boolean failOnError;

    /**
     * Default PubSub Sink constructor that creates a new {@link PubSubSinkV2} with the provided
     * configurations.
     *
     * @param projectId the GCP project ID.
     * @param topicId the PubSub topic ID.
     * @param serializationSchema the serialization schema to serialize input elements into PubSub
     *     messages.
     * @param publisherConfig the configuration for the PubSub publisher.
     * @param maxInFlightRequests the maximum number of inflight requests.
     * @param failOnError flag to indicate whether to fail on errors.
     */
    public PubSubSinkV2(
            String projectId,
            String topicId,
            SerializationSchema<T> serializationSchema,
            GcpPublisherConfig publisherConfig,
            int maxInFlightRequests,
            boolean failOnError) {
        Preconditions.checkNotNull(projectId, "Project ID cannot be null.");
        Preconditions.checkNotNull(topicId, "Topic ID cannot be null.");
        Preconditions.checkNotNull(publisherConfig, "Connection configs cannot be null.");
        Preconditions.checkNotNull(serializationSchema, "Serialization schema cannot be null.");
        Preconditions.checkArgument(
                maxInFlightRequests > 0, "Max inflight requests must be greater than 0.");

        this.projectId = projectId;
        this.topicId = topicId;
        this.serializationSchema = serializationSchema;
        this.publisherConfig = publisherConfig;
        this.maxInFlightRequests = maxInFlightRequests;
        this.failOnError = failOnError;
    }

    @Override
    public SinkWriter<T> createWriter(InitContext initContext) throws IOException {
        throw new UnsupportedOperationException(
                "Deprecated method. Use createWriter(WriterInitContext) instead.");
    }

    @Override
    public PubSubWriter<T> createWriter(WriterInitContext initContext) throws IOException {
        return new PubSubWriter<>(
                projectId,
                topicId,
                serializationSchema,
                initContext,
                publisherConfig,
                maxInFlightRequests,
                failOnError);
    }

    public static <T> PubSubSinkV2Builder<T> builder() {
        return new PubSubSinkV2Builder<>();
    }
}
