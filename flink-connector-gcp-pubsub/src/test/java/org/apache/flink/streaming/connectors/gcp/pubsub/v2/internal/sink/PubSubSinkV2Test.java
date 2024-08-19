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

package org.apache.flink.streaming.connectors.gcp.pubsub.v2.internal.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.PubSubSerializationSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.v2.PubSubSink;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertThrows;

@RunWith(MockitoJUnitRunner.class)
public class PubSubSinkV2Test {
    @Test
    public void build_invalidTopic() throws Exception {
        assertThrows(
                NullPointerException.class, () -> PubSubSink.<String>builder().setTopicName(null));
        PubSubSink.Builder<String> builder =
                PubSubSink.<String>builder()
                        .setProjectName("project")
                        .setSerializationSchema(
                                PubSubSerializationSchema.dataOnly(new SimpleStringSchema()));
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    public void build_invalidProject() throws Exception {
        assertThrows(
                NullPointerException.class,
                () -> PubSubSink.<String>builder().setProjectName(null));
        PubSubSink.Builder<String> builder =
                PubSubSink.<String>builder()
                        .setTopicName("topic")
                        .setSerializationSchema(
                                PubSubSerializationSchema.dataOnly(new SimpleStringSchema()));
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    public void build_invalidSchema() throws Exception {
        assertThrows(
                NullPointerException.class,
                () -> PubSubSink.<String>builder().setSerializationSchema(null));
        PubSubSink.Builder<String> builder =
                PubSubSink.<String>builder().setProjectName("project").setTopicName("topic");
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    public void build_invalidCreds() throws Exception {
        assertThrows(
                NullPointerException.class,
                () -> PubSubSink.<String>builder().setCredentials(null));
    }

    @Test
    public void build_invalidEnableMessageOrdering() throws Exception {
        assertThrows(
                NullPointerException.class,
                () -> PubSubSink.<String>builder().setEnableMessageOrdering(null));
    }

    @Test
    public void build_invalidEndpoint() throws Exception {
        assertThrows(
                NullPointerException.class, () -> PubSubSink.<String>builder().setEndpoint(null));
    }
}
