/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.gcp.pubsub;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.PubSubDeserializationSchemaV2;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.PubSubSource;

import com.google.pubsub.v1.ProjectSubscriptionName;

/**
 * A simple example PubSub Flink job using the v2 source.
 *
 * <p>This job pulls messages from a PubSub subscription as a data source and prints the message.
 *
 * <p>Example usage (source subscription and sink topic must be in the same GCP project): $ flink
 * run PubSubSourceV2Example.jar --project PROJECT-NAME --source-subscription SUBSCRIPTION-NAME
 *
 * <p>Example usage (project can be parsed off of the source subscription instead of specifying the
 * project explicitly): $ flink run PubSubSourceV2Example.jar --source-subscription
 * projects/PROJECT-NAME/subscriptions/SUBSCRIPTION-NAME
 */
public class PubSubSourceV2Example {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // Parse source subscription from parameters.
        String subscriptionName = parameterTool.get("source-subscription");
        String subscriptionProject = parameterTool.get("project");
        if (subscriptionName == null) {
            System.out.println(
                    "Failed to start! The parameter --source-subscription must be specified.");
        }
        if (ProjectSubscriptionName.isParsableFrom(subscriptionName)) {
            ProjectSubscriptionName projectSubscriptionName =
                    ProjectSubscriptionName.parse(subscriptionName);
            subscriptionName = projectSubscriptionName.getSubscription();
            subscriptionProject = projectSubscriptionName.getProject();
        }
        if (subscriptionProject == null) {
            System.out.println(
                    "Failed to start! The source subscription project must be specified using either"
                            + " [--project PROJECT-NAME] or [--source-subscription"
                            + " projects/PROJECT-NAME/subscriptions/SUBSCRIPTION-NAME].");
            return;
        }

        DataStream<String> stream =
                env.fromSource(
                        PubSubSource.<String>builder()
                                .setDeserializationSchema(
                                        PubSubDeserializationSchemaV2.dataOnly(
                                                new SimpleStringSchema()))
                                .setProjectName(subscriptionProject)
                                .setSubscriptionName(subscriptionName)
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "PubSubSource");
        stream.print();

        // Start a checkpoint every 1000 ms.
        env.enableCheckpointing(1000);
        env.execute("Streaming PubSub Example");
    }
}