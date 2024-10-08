package org.apache.flink.connector.gcp.pubsub.sink.config;

import org.apache.flink.annotation.PublicEvolving;

import com.google.api.gax.rpc.TransportChannelProvider;

import java.io.Serializable;

/**
 * A serializable transport channel provider for {@link
 * org.apache.flink.connector.gcp.pubsub.sink.GcpWriterClient}.
 */
@PublicEvolving
public abstract class SerializableTransportChannelProvider implements Serializable {
    private static final long serialVersionUID = 1L;

    protected transient TransportChannelProvider transportChannelProvider;

    protected abstract void open();

    public TransportChannelProvider getTransportChannelProvider() {
        if (transportChannelProvider == null) {
            open();
        }
        return transportChannelProvider;
    }
}
