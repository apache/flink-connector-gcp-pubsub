package org.apache.flink.connector.gcp.pubsub.sink;

import org.apache.flink.annotation.PublicEvolving;

/** Exception thrown when an error occurs during PubSub message handling. */
@PublicEvolving
public class PubSubException extends RuntimeException {
    public PubSubException(String message, Throwable cause) {
        super(message, cause);
    }
}
