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
import org.apache.flink.connector.gcp.pubsub.sink.PubSubSinkV2Builder;
import org.apache.flink.connector.gcp.pubsub.sink.config.GcpPublisherConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * PubSub implementation of {@link DynamicTableSink} that provides {@link
 * org.apache.flink.connector.gcp.pubsub.sink.PubSubSinkV2}.
 */
@Internal
class PubSubDynamicSink implements DynamicTableSink {

    /** Data type that describes the final output of the source. */
    private final DataType consumedDataType;

    /** Google Cloud project id. */
    private final String projectId;

    /** Google Cloud Pub/Sub topic id for the table. */
    private final String topic;

    /** Google Cloud Publisher configuration. */
    private final GcpPublisherConfig gcpPublisherConfig;

    /** Format for encoding values to pubsub. */
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

    /** Maximum number of in-flight requests handled by sink. */
    private final Integer numMaxInflightRequests;

    /** Flag to determine if the sink should fail on errors. */
    private final Boolean failOnError;

    public PubSubDynamicSink(
            DataType consumedDataType,
            String projectId,
            String topic,
            GcpPublisherConfig gcpPublisherConfig,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            @Nullable Integer numMaxInflightRequests,
            @Nullable Boolean failOnError) {
        this.consumedDataType = consumedDataType;
        this.projectId = projectId;
        this.topic = topic;
        this.gcpPublisherConfig = gcpPublisherConfig;
        this.encodingFormat = encodingFormat;
        this.numMaxInflightRequests = numMaxInflightRequests;
        this.failOnError = failOnError;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        // the PubSub sink only supports insert-only mode
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        PubSubSinkV2Builder<RowData> builder =
                new PubSubSinkV2Builder<RowData>()
                        .setSerializationSchema(
                                encodingFormat.createRuntimeEncoder(context, consumedDataType))
                        .setProjectId(projectId)
                        .setTopicId(topic)
                        .setGcpPublisherConfig(gcpPublisherConfig);
        Optional.ofNullable(numMaxInflightRequests).ifPresent(builder::setNumMaxInflightRequests);
        Optional.ofNullable(failOnError).ifPresent(builder::setFailOnError);

        return SinkV2Provider.of(builder.build());
    }

    @Override
    public DynamicTableSink copy() {
        return new PubSubDynamicSink(
                consumedDataType,
                projectId,
                topic,
                gcpPublisherConfig,
                encodingFormat,
                numMaxInflightRequests,
                failOnError);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PubSubDynamicSink that = (PubSubDynamicSink) o;

        return Objects.equals(projectId, that.projectId)
                && Objects.equals(topic, that.topic)
                && Objects.equals(gcpPublisherConfig, that.gcpPublisherConfig)
                && Objects.equals(encodingFormat, that.encodingFormat)
                && Objects.equals(numMaxInflightRequests, that.numMaxInflightRequests)
                && Objects.equals(failOnError, that.failOnError);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                projectId,
                topic,
                gcpPublisherConfig,
                encodingFormat,
                numMaxInflightRequests,
                failOnError);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }

    @Override
    public String asSummaryString() {
        return "PubSubDynamicSink{"
                + "projectId='"
                + projectId
                + '\''
                + ", topic='"
                + topic
                + '\''
                + ", numMaxInflightRequests="
                + numMaxInflightRequests
                + ", config="
                + gcpPublisherConfig.toString()
                + ", failOnError="
                + failOnError
                + '}';
    }
}
