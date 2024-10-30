package org.apache.flink.connector.gcp.pubsub.sink.config;

import org.apache.flink.annotation.PublicEvolving;

import com.google.api.gax.rpc.TransportChannelProvider;

import java.io.Serializable;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SerializableTransportChannelProvider that = (SerializableTransportChannelProvider) o;

        return Objects.equals(transportChannelProvider, that.transportChannelProvider);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transportChannelProvider);
    }
}
