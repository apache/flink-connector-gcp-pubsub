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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.gcp.pubsub.sink.PubSubSinkV2;
import org.apache.flink.connector.gcp.pubsub.sink.config.GcpPublisherConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TableOptionsBuilder;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.retrying.RetrySettings;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.threeten.bp.Duration;

import java.util.Map;

import static com.google.cloud.pubsub.v1.SubscriptionAdminSettings.defaultCredentialsProviderBuilder;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;

/** Tests for {@link org.apache.flink.connector.gcp.pubsub.table.PubSubDynamicSinkFactory}. */
class PubSubDynamicSinkFactoryTest {
    private static final String PROJECT = "test-project";
    private static final String TOPIC = "test-topic";

    @Test
    void goodTableSinkDefaultTableConfig() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        DataType physicalDataType = sinkSchema.toPhysicalRowDataType();
        Map<String, String> sinkOptions = defaultTableOptions().build();

        // Construct actual DynamicTableSink using FactoryUtil
        PubSubDynamicSink actualSink = (PubSubDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected DynamicTableSink using factory under test
        PubSubDynamicSink expectedSink = constructExpectedSink(physicalDataType, null, false);

        assertTableSinkEqualsAndOfCorrectType(actualSink, expectedSink);
    }

    @Test
    void tableSinkWithMaxInflightOverride() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        DataType physicalDataType = sinkSchema.toPhysicalRowDataType();
        Map<String, String> sinkOptions =
                defaultTableOptions().withTableOption("sink.inflight-requests.max", "10").build();

        // Construct actual DynamicTableSink using FactoryUtil
        PubSubDynamicSink actualSink = (PubSubDynamicSink) createTableSink(sinkSchema, sinkOptions);
        // Construct expected DynamicTableSink using factory under test
        PubSubDynamicSink expectedSink = constructExpectedSink(physicalDataType, 10, false);
        assertTableSinkEqualsAndOfCorrectType(actualSink, expectedSink);
    }

    @Test
    void tableSinkWithFailOnError() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        DataType physicalDataType = sinkSchema.toPhysicalRowDataType();
        Map<String, String> sinkOptions =
                defaultTableOptions().withTableOption("sink.fail-on-error", "true").build();

        // Construct actual DynamicTableSink using FactoryUtil
        PubSubDynamicSink actualSink = (PubSubDynamicSink) createTableSink(sinkSchema, sinkOptions);
        // Construct expected DynamicTableSink using factory under test
        PubSubDynamicSink expectedSink = constructExpectedSink(physicalDataType, null, true);
        assertTableSinkEqualsAndOfCorrectType(actualSink, expectedSink);
    }

    @Test
    void tableSinkWithFixedCredentialProviderFailsIfCredentialsFileNotProvided() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                defaultTableOptions().withTableOption("credentials-provider.type", "fixed").build();

        // Construct actual DynamicTableSink using FactoryUtil
        Assertions.assertThatThrownBy(() -> createTableSink(sinkSchema, sinkOptions))
                .cause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Fixed credential provider requires 'credentials-provider.fixed.credentials-json' or 'credentials-provider.fixed.credentials.access-token' options to be set");
    }

    @Test
    void tableSinkWithGoogleCredentialsSucceeds() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        DataType physicalDataType = sinkSchema.toPhysicalRowDataType();
        Map<String, String> sinkOptions =
                defaultTableOptions()
                        .withTableOption("credentials-provider.type", "google-credentials")
                        .build();

        // Construct actual DynamicTableSink using FactoryUtil
        PubSubDynamicSink actualSink = (PubSubDynamicSink) createTableSink(sinkSchema, sinkOptions);
        // Construct expected DynamicTableSink using factory under test
        PubSubDynamicSink expectedSink = constructExpectedSink(physicalDataType, null, false);
        assertTableSinkEqualsAndOfCorrectType(actualSink, expectedSink);
    }

    @Test
    void tableSinkWithBadCredentialProviderType() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                defaultTableOptions().withTableOption("credentials-provider.type", "bad").build();

        // Construct actual DynamicTableSink using FactoryUtil
        Assertions.assertThatThrownBy(() -> createTableSink(sinkSchema, sinkOptions))
                .cause()
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Invalid value for option 'credentials-provider.type'");
    }

    @Test
    void tableSinkWithBatchSettingsEnabled() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        DataType physicalDataType = sinkSchema.toPhysicalRowDataType();
        Map<String, String> sinkOptions =
                defaultTableOptions()
                        .withTableOption("sink.batching.enabled", "true")
                        .withTableOption("sink.batching.element-count-threshold", "100")
                        .withTableOption("sink.batching.request-byte-threshold", "1000")
                        .withTableOption("sink.batching.delay-threshold-millis", "1000")
                        .withTableOption("sink.batching.max-outstanding-element-count", "1000")
                        .withTableOption("sink.batching.max-outstanding-request-bytes", "10000")
                        .build();

        // Construct actual DynamicTableSink using FactoryUtil
        PubSubDynamicSink actualSink = (PubSubDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected DynamicTableSink using factory under test
        PubSubDynamicSink expectedSink =
                constructExpectedSink(
                        physicalDataType,
                        GcpPublisherConfig.builder()
                                .setCredentialsProvider(defaultCredentialsProviderBuilder().build())
                                .setBatchingSettings(
                                        BatchingSettings.newBuilder()
                                                .setDelayThreshold(Duration.ofMillis(1000))
                                                .setElementCountThreshold(100L)
                                                .setRequestByteThreshold(1000L)
                                                .setFlowControlSettings(
                                                        FlowControlSettings.newBuilder()
                                                                .setMaxOutstandingElementCount(
                                                                        1000L)
                                                                .setMaxOutstandingRequestBytes(
                                                                        10000L)
                                                                .build())
                                                .setIsEnabled(true)
                                                .build())
                                .build(),
                        null,
                        false);

        assertTableSinkEqualsAndOfCorrectType(actualSink, expectedSink);
    }

    @Test
    void tableSinkWithRetrySettings() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        DataType physicalDataType = sinkSchema.toPhysicalRowDataType();
        Map<String, String> sinkOptions =
                defaultTableOptions()
                        .withTableOption("sink.retry.initial-delay-millis", "1000")
                        .withTableOption("sink.retry.max-attempts", "10")
                        .withTableOption("sink.retry.max-delay-millis", "10000")
                        .build();

        // Construct actual DynamicTableSink using FactoryUtil
        PubSubDynamicSink actualSink = (PubSubDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected DynamicTableSink using factory under test
        PubSubDynamicSink expectedSink =
                constructExpectedSink(
                        physicalDataType,
                        GcpPublisherConfig.builder()
                                .setCredentialsProvider(defaultCredentialsProviderBuilder().build())
                                .setRetrySettings(
                                        RetrySettings.newBuilder()
                                                .setInitialRetryDelay(Duration.ofMillis(1000))
                                                .setMaxAttempts(10)
                                                .setMaxRetryDelay(Duration.ofMillis(10000))
                                                .build())
                                .build(),
                        null,
                        false);
        assertTableSinkEqualsAndOfCorrectType(actualSink, expectedSink);
    }

    @Test
    void tableSinkWithCompressionEnabled() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        DataType physicalDataType = sinkSchema.toPhysicalRowDataType();
        Map<String, String> sinkOptions =
                defaultTableOptions().withTableOption("sink.compression.enabled", "true").build();

        // Construct actual DynamicTableSink using FactoryUtil
        PubSubDynamicSink actualSink = (PubSubDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected DynamicTableSink using factory under test
        PubSubDynamicSink expectedSink =
                constructExpectedSink(
                        physicalDataType,
                        GcpPublisherConfig.builder()
                                .setCredentialsProvider(defaultCredentialsProviderBuilder().build())
                                .setEnableCompression(true)
                                .build(),
                        null,
                        false);
        assertTableSinkEqualsAndOfCorrectType(actualSink, expectedSink);
    }

    private ResolvedSchema defaultSinkSchema() {
        return ResolvedSchema.of(
                Column.physical("name", DataTypes.STRING()),
                Column.physical("curr_id", DataTypes.BIGINT()),
                Column.physical("time", DataTypes.TIMESTAMP(3)));
    }

    private TableOptionsBuilder defaultTableOptions() {
        String connector = PubSubDynamicSinkFactory.IDENTIFIER;
        String format = TestFormatFactory.IDENTIFIER;
        return new TableOptionsBuilder(connector, format)
                // default table options
                .withTableOption("project", PROJECT)
                .withTableOption("topic", TOPIC)
                .withFormatOption(TestFormatFactory.DELIMITER, ",")
                .withFormatOption(TestFormatFactory.FAIL_ON_MISSING, "true");
    }

    private PubSubDynamicSink constructExpectedSink(
            DataType physicalDataType, Integer maxInflightRequests, boolean failOnError) {
        return constructExpectedSink(
                physicalDataType,
                GcpPublisherConfig.builder()
                        .setCredentialsProvider(defaultCredentialsProviderBuilder().build())
                        .build(),
                maxInflightRequests,
                failOnError);
    }

    private PubSubDynamicSink constructExpectedSink(
            DataType physicalDataType,
            GcpPublisherConfig gcpPublisherConfig,
            Integer maxInflightRequests,
            boolean failOnError) {
        return new PubSubDynamicSink(
                physicalDataType,
                PROJECT,
                TOPIC,
                gcpPublisherConfig,
                new TestFormatFactory.EncodingFormatMock(","),
                maxInflightRequests,
                failOnError);
    }

    private void assertTableSinkEqualsAndOfCorrectType(
            DynamicTableSink actualSink, DynamicTableSink expectedSink) {
        // verify that the constructed DynamicTableSink is as expected
        Assertions.assertThat(actualSink).isEqualTo(expectedSink);

        // verify the produced sink
        DynamicTableSink.SinkRuntimeProvider sinkFunctionProvider =
                actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        Sink<RowData> sinkFunction = ((SinkV2Provider) sinkFunctionProvider).createSink();
        Assertions.assertThat(sinkFunction).isInstanceOf(PubSubSinkV2.class);
    }
}
