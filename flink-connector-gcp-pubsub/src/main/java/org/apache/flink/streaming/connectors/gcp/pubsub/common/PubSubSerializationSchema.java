package org.apache.flink.streaming.connectors.gcp.pubsub.common;

import org.apache.flink.api.common.serialization.SerializationSchema;

import java.util.Map;

/**
 * The deserialization schema describes how to turn the PubsubMessages into data types (Java/Scala
 * objects) that are processed by Flink.
 *
 * @param <T> The type created by the deserialization schema.
 */
public interface PubSubSerializationSchema<T> extends SerializationSchema<T> {
    /**
     * Get a map of attributes to be set on the PubsubMessage.
     *
     * @return the key-value map of attributes for the PubsubMessage.
     */
    Map<String, String> getAttributesMap(T element);
}
