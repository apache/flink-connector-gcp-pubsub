package org.apache.flink.connector.gcp.pubsub.sink;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;

/**
 * Abstract interface for GCP writer client to decouple dependency of writer on publisher
 * implementation.
 *
 * @param <T> The type of the messages to write.
 */
@Internal
public interface GcpWriterClient<T> {

    ApiFuture<String> publish(T var1);

    /**
     * This method is called to flush all outstanding messages. This is used when client is
     * configured with batching.
     */
    void flush();

    void shutdown();
}
