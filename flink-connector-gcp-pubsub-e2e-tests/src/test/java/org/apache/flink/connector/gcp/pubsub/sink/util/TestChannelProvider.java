package org.apache.flink.connector.gcp.pubsub.sink.util;

import org.apache.flink.connector.gcp.pubsub.sink.config.SerializableTransportChannelProvider;

import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/** A test channel provider for {@link GrpcTransportChannel}. */
public class TestChannelProvider extends SerializableTransportChannelProvider {

    private final String endpoint;

    public TestChannelProvider(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    protected void open() {
        ManagedChannel managedChannel =
                ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
        this.transportChannelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(managedChannel));
    }
}
