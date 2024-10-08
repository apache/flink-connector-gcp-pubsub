package org.apache.flink.connector.gcp.pubsub.sink;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;

/** A {@link GcpWriterClient} implementation for PubSub {@link Publisher}. */
@Internal
public class PubSubWriterClient implements GcpWriterClient<PubsubMessage> {

    private final Publisher publisher;

    private PubSubWriterClient(Publisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void flush() {
        publisher.publishAllOutstanding();
    }

    @Override
    public void shutdown() {
        publisher.shutdown();
    }

    @Override
    public ApiFuture<String> publish(PubsubMessage pubsubMessage) {
        return publisher.publish(pubsubMessage);
    }

    public static PubSubWriterClient of(Publisher publisher) {
        return new PubSubWriterClient(publisher);
    }
}
