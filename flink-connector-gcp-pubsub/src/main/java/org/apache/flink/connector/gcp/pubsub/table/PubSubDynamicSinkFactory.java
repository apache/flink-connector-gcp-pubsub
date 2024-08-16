/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.gcp.pubsub.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.gcp.pubsub.sink.config.GcpPublisherConfig;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.retrying.RetrySettings;
import org.threeten.bp.Duration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.gcp.pubsub.table.PubSubOptions.CREDENTIALS_PROVIDER;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

/** Factory for creating {@link PubSubDynamicSink}. */
@Internal
public class PubSubDynamicSinkFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "pubsub";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FORMAT);
        helper.validate();
        ReadableConfig config = helper.getOptions();
        return new PubSubDynamicSink(
                catalogTable.getResolvedSchema().toPhysicalRowDataType(),
                config.get(PubSubOptions.PROJECT),
                config.get(PubSubOptions.TOPIC),
                getResolvedPublisherConfig(config),
                format,
                config.get(PubSubOptions.MAX_IN_FLIGHT_REQUESTS),
                config.get(PubSubOptions.FAIL_ON_ERROR));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PubSubOptions.PROJECT);
        options.add(PubSubOptions.TOPIC);
        options.add(FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PubSubOptions.CREDENTIALS_PROVIDER_TYPE);
        options.add(PubSubOptions.CREDENTIALS_FIXED_JSON);
        options.add(PubSubOptions.CREDENTIALS_FIXED_ACCESS_TOKEN);
        options.add(PubSubOptions.CREDENTIALS_FIXED_ACCESS_EXPIRATION_EPOCH_MILLIS);
        options.add(PubSubOptions.MAX_IN_FLIGHT_REQUESTS);
        options.add(CREDENTIALS_PROVIDER);
        options.add(PubSubOptions.FAIL_ON_ERROR);
        options.add(PubSubOptions.BATCHING_ENABLED);
        options.add(PubSubOptions.BATCHING_ELEMENT_COUNT_THRESHOLD);
        options.add(PubSubOptions.BATCHING_DELAY_THRESHOLD_MS);
        options.add(PubSubOptions.BATCHING_REQUEST_BYTE_THRESHOLD);
        options.add(PubSubOptions.BATCHING_MAX_OUTSTANDING_ELEMENT_COUNT);
        options.add(PubSubOptions.BATCHING_MAX_OUTSTANDING_REQUEST_BYTES);
        options.add(PubSubOptions.RETRY_SETTINGS_INITIAL_DELAY_MS);
        options.add(PubSubOptions.RETRY_SETTINGS_MAX_ATTEMPTS);
        options.add(PubSubOptions.RETRY_SETTINGS_MAX_DELAY_MS);
        options.add(PubSubOptions.COMPRESSION_ENABLED);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PubSubOptions.MAX_IN_FLIGHT_REQUESTS);
        options.add(PubSubOptions.FAIL_ON_ERROR);
        options.add(PubSubOptions.BATCHING_ENABLED);
        options.add(PubSubOptions.BATCHING_ELEMENT_COUNT_THRESHOLD);
        options.add(PubSubOptions.BATCHING_DELAY_THRESHOLD_MS);
        options.add(PubSubOptions.BATCHING_REQUEST_BYTE_THRESHOLD);
        options.add(PubSubOptions.BATCHING_MAX_OUTSTANDING_ELEMENT_COUNT);
        options.add(PubSubOptions.BATCHING_MAX_OUTSTANDING_REQUEST_BYTES);
        options.add(PubSubOptions.RETRY_SETTINGS_INITIAL_DELAY_MS);
        options.add(PubSubOptions.RETRY_SETTINGS_MAX_ATTEMPTS);
        options.add(PubSubOptions.RETRY_SETTINGS_MAX_DELAY_MS);
        options.add(PubSubOptions.COMPRESSION_ENABLED);
        return options;
    }

    private GcpPublisherConfig getResolvedPublisherConfig(ReadableConfig config) {
        CredentialProviderFactory credentialProviderFactory = new CredentialProviderFactory();
        CredentialProviderType credentialProviderType =
                config.get(PubSubOptions.CREDENTIALS_PROVIDER_TYPE);
        Map<String, String> credentialProviderOptions =
                Optional.ofNullable(config.get(CREDENTIALS_PROVIDER))
                        .orElse(Collections.emptyMap());

        GcpPublisherConfig.Builder builder =
                GcpPublisherConfig.builder()
                        .setCredentialsProvider(
                                credentialProviderFactory.createCredentialsProvider(
                                        credentialProviderType, credentialProviderOptions));

        Optional.ofNullable(getResolvedRetrySettings(config)).ifPresent(builder::setRetrySettings);
        Optional.ofNullable(getResolvedBatchingSettings(config))
                .ifPresent(builder::setBatchingSettings);
        Optional.ofNullable(config.get(PubSubOptions.COMPRESSION_ENABLED))
                .ifPresent(builder::setEnableCompression);
        return builder.build();
    }

    private BatchingSettings getResolvedBatchingSettings(ReadableConfig config) {
        BatchingSettings.Builder builder = BatchingSettings.newBuilder();
        Optional<Long> batchCountThresholdOptional =
                Optional.ofNullable(config.get(PubSubOptions.BATCHING_ELEMENT_COUNT_THRESHOLD));
        Optional<Long> batchDelayThresholdOptional =
                Optional.ofNullable(config.get(PubSubOptions.BATCHING_DELAY_THRESHOLD_MS));
        Optional<Boolean> batchEnabledOptional =
                Optional.ofNullable(config.get(PubSubOptions.BATCHING_ENABLED));
        Optional<Long> batchRequestByteThresholdOptional =
                Optional.ofNullable(config.get(PubSubOptions.BATCHING_REQUEST_BYTE_THRESHOLD));

        Optional<Long> maxOutstandingElementCount =
                Optional.ofNullable(
                        config.get(PubSubOptions.BATCHING_MAX_OUTSTANDING_ELEMENT_COUNT));
        Optional<Long> maxOutstandingRequestBytes =
                Optional.ofNullable(
                        config.get(PubSubOptions.BATCHING_MAX_OUTSTANDING_REQUEST_BYTES));

        if (!batchCountThresholdOptional.isPresent()
                && !batchDelayThresholdOptional.isPresent()
                && (!batchEnabledOptional.isPresent() || !batchEnabledOptional.get())
                && !batchRequestByteThresholdOptional.isPresent()
                && !maxOutstandingElementCount.isPresent()
                && !maxOutstandingRequestBytes.isPresent()) {

            return null;
        }

        batchCountThresholdOptional.ifPresent(builder::setElementCountThreshold);
        batchDelayThresholdOptional.ifPresent(
                delay -> builder.setDelayThreshold(Duration.ofMillis(delay)));
        batchEnabledOptional.ifPresent(builder::setIsEnabled);
        batchRequestByteThresholdOptional.ifPresent(builder::setRequestByteThreshold);
        if (maxOutstandingElementCount.isPresent() || maxOutstandingRequestBytes.isPresent()) {
            FlowControlSettings.Builder flowControlSettingsBuilder =
                    FlowControlSettings.newBuilder();
            maxOutstandingElementCount.ifPresent(
                    flowControlSettingsBuilder::setMaxOutstandingElementCount);
            maxOutstandingRequestBytes.ifPresent(
                    flowControlSettingsBuilder::setMaxOutstandingRequestBytes);
            builder.setFlowControlSettings(flowControlSettingsBuilder.build());
        }

        return builder.build();
    }

    private RetrySettings getResolvedRetrySettings(ReadableConfig config) {
        Optional<Long> initialDelayOptional =
                Optional.ofNullable(config.get(PubSubOptions.RETRY_SETTINGS_INITIAL_DELAY_MS));
        Optional<Integer> maxAttemptsOptional =
                Optional.ofNullable(config.get(PubSubOptions.RETRY_SETTINGS_MAX_ATTEMPTS));
        Optional<Long> maxDelayOptional =
                Optional.ofNullable(config.get(PubSubOptions.RETRY_SETTINGS_MAX_DELAY_MS));
        if (!initialDelayOptional.isPresent()
                && !maxAttemptsOptional.isPresent()
                && !maxDelayOptional.isPresent()) {
            return null;
        }

        RetrySettings.Builder builder = RetrySettings.newBuilder();
        initialDelayOptional.ifPresent(
                delay -> builder.setInitialRetryDelay(Duration.ofMillis(delay)));
        maxAttemptsOptional.ifPresent(builder::setMaxAttempts);
        maxDelayOptional.ifPresent(delay -> builder.setMaxRetryDelay(Duration.ofMillis(delay)));
        return builder.build();
    }
}
