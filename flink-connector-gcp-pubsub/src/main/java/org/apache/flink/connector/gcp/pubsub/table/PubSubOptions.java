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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Map;

/** Options for the Pub/Sub connector. */
@PublicEvolving
public class PubSubOptions {
    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the Pub/Sub topic backing this table.");

    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("GCP project ID.");

    public static final ConfigOption<Integer> MAX_IN_FLIGHT_REQUESTS =
            ConfigOptions.key("sink.inflight-requests.max")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Maximum number of inflight requests.");

    public static final ConfigOption<Boolean> FAIL_ON_ERROR =
            ConfigOptions.key("sink.fail-on-error")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Flag to trigger global failure on error.");

    public static final ConfigOption<Boolean> BATCHING_ENABLED =
            ConfigOptions.key("sink.batching.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Flag to enable batching.");

    public static final ConfigOption<Long> BATCHING_ELEMENT_COUNT_THRESHOLD =
            ConfigOptions.key("sink.batching.element-count-threshold")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Batching settings Element count threshold.");

    public static final ConfigOption<Long> BATCHING_DELAY_THRESHOLD_MS =
            ConfigOptions.key("sink.batching.delay-threshold-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Batching settings delay threshold in milliseconds.");

    public static final ConfigOption<Long> BATCHING_REQUEST_BYTE_THRESHOLD =
            ConfigOptions.key("sink.batching.request-byte-threshold")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Batching settings request byte threshold.");

    public static final ConfigOption<Long> BATCHING_MAX_OUTSTANDING_ELEMENT_COUNT =
            ConfigOptions.key("sink.batching.max-outstanding-element-count")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Batching settings max outstanding element count.");

    public static final ConfigOption<Long> BATCHING_MAX_OUTSTANDING_REQUEST_BYTES =
            ConfigOptions.key("sink.batching.max-outstanding-request-bytes")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Batching settings max outstanding request bytes.");

    public static final ConfigOption<Long> RETRY_SETTINGS_INITIAL_DELAY_MS =
            ConfigOptions.key("sink.retry.initial-delay-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Retry settings initial delay.");

    public static final ConfigOption<Integer> RETRY_SETTINGS_MAX_ATTEMPTS =
            ConfigOptions.key("sink.retry.max-attempts")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Retry settings maximum retry attempts.");

    public static final ConfigOption<Long> RETRY_SETTINGS_MAX_DELAY_MS =
            ConfigOptions.key("sink.retry.max-delay-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Retry settings max delay.");

    public static final ConfigOption<Map<String, String>> CREDENTIALS_PROVIDER =
            ConfigOptions.key("credentials-provider")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Credentials provider options.");

    public static final ConfigOption<CredentialProviderType> CREDENTIALS_PROVIDER_TYPE =
            ConfigOptions.key("credentials-provider.type")
                    .enumType(CredentialProviderType.class)
                    .defaultValue(CredentialProviderType.DEFAULT)
                    .withDescription("Type of the GCP credentials provider.");

    public static final ConfigOption<String> CREDENTIALS_FIXED_JSON =
            ConfigOptions.key("credentials-provider.fixed.credentials-json")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Json formatted string of the GCP credentials for fixed credentials provider.");

    public static final ConfigOption<String> CREDENTIALS_FIXED_ACCESS_TOKEN =
            ConfigOptions.key("credentials-provider.fixed.credentials.access-token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Access token of GCP credentials for fixed credentials provider.");

    public static final ConfigOption<Long> CREDENTIALS_FIXED_ACCESS_EXPIRATION_EPOCH_MILLIS =
            ConfigOptions.key(
                            "credentials-provider.fixed.credentials.access-expiration-epoch-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Expiration time epoch in millis of GCP credentials for fixed credentials provider.");

    public static final ConfigOption<Boolean> COMPRESSION_ENABLED =
            ConfigOptions.key("sink.compression.enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Flag to enable compression for the Pub/Sub messages.");

    private PubSubOptions() {}
}
