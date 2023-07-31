package org.apache.flink.streaming.connectors.gcp.pubsub;

import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSerializationSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * Test sink serialization schema. Test Schema will add an attribute to the PubSubMessage with the
 * value of the element.
 */
public class PubSubSinkSerializationTestStringSchema implements PubSubSerializationSchema<String> {

    @Override
    public Map<String, String> getAttributesMap(String element) {
        return new HashMap<String, String>() {
            {
                put("attribute1", element);
            }
        };
    }

    @Override
    public byte[] serialize(String element) {
        return element.getBytes();
    }
}
