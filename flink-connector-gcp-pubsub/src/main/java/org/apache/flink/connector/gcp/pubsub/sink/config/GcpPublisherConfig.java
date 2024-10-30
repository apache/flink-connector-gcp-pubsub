package org.apache.flink.connector.gcp.pubsub.sink.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.TransportChannelProvider;

import java.io.Serializable;

/** Configuration keys for {@link com.google.cloud.pubsub.v1.Publisher}. */
@PublicEvolving
public class GcpPublisherConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final RetrySettings retrySettings;

    private final BatchingSettings batchingSettings;

    private final CredentialsProvider credentialsProvider;

    private final SerializableTransportChannelProvider transportChannelProvider;

    private final Boolean enableCompression;

    public GcpPublisherConfig(
            RetrySettings retrySettings,
            BatchingSettings batchingSettings,
            CredentialsProvider credentialsProvider,
            SerializableTransportChannelProvider transportChannelProvider,
            Boolean enableCompression) {
        Preconditions.checkNotNull(credentialsProvider, "Credentials provider cannot be null");
        this.retrySettings = retrySettings;
        this.batchingSettings = batchingSettings;
        this.credentialsProvider = credentialsProvider;
        this.transportChannelProvider = transportChannelProvider;
        this.enableCompression = enableCompression;
    }

    public RetrySettings getRetrySettings() {
        return retrySettings;
    }

    public BatchingSettings getBatchingSettings() {
        return batchingSettings;
    }

    public CredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    public TransportChannelProvider getTransportChannelProvider() {
        if (transportChannelProvider == null) {
            return null;
        }
        return transportChannelProvider.getTransportChannelProvider();
    }

    public Boolean getEnableCompression() {
        return enableCompression;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link GcpPublisherConfig}. */
    public static class Builder {

        private RetrySettings retrySettings;

        private BatchingSettings batchingSettings;

        private CredentialsProvider credentialsProvider;

        private SerializableTransportChannelProvider transportChannelProvider;

        private Boolean enableCompression;

        public Builder setRetrySettings(RetrySettings retrySettings) {
            this.retrySettings = retrySettings;
            return this;
        }

        public Builder setBatchingSettings(BatchingSettings batchingSettings) {
            this.batchingSettings = batchingSettings;
            return this;
        }

        public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
            this.credentialsProvider = credentialsProvider;
            return this;
        }

        public Builder setTransportChannelProvider(
                SerializableTransportChannelProvider transportChannelProvider) {
            this.transportChannelProvider = transportChannelProvider;
            return this;
        }

        public Builder setEnableCompression(boolean enableCompression) {
            this.enableCompression = enableCompression;
            return this;
        }

        public GcpPublisherConfig build() {
            return new GcpPublisherConfig(
                    retrySettings,
                    batchingSettings,
                    credentialsProvider,
                    transportChannelProvider,
                    enableCompression);
        }
    }
}
