---
title: Google Cloud PubSub
weight: 8
type: docs
aliases:
  - /dev/connectors/pubsub.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Google Cloud PubSub

This connector provides a Source and Sink that can read from and write to
[Google Cloud PubSub](https://cloud.google.com/pubsub). To use this connector, add the
following dependency to your project:

{{< connector_artifact flink-connector-gcp-pubsub pubsub >}}

{{< hint warning >}}
<b>Note</b>: This connector has been added to Flink recently. It has not received widespread testing yet.
{{< /hint >}}


Note that the streaming connectors are currently not part of the binary
distribution. See
[here]({{< ref "docs/dev/configuration/overview" >}})
for information about how to package the program with the libraries for
cluster execution.

## Consuming or Producing PubSubMessages

The connector provides a connectors for receiving and sending messages from and to Google PubSub.
Google PubSub has an `at-least-once` guarantee and as such the connector delivers the same guarantees.

### PubSub SourceFunction

The class `PubSubSource` has a builder to create PubSubsources: `PubSubSource.newBuilder(...)`

There are several optional methods to alter how the PubSubSource is created, the bare minimum is to provide a Google project, Pubsub subscription and a way to deserialize the PubSubMessages.

Example:

{{< tabs "f5af7878-b460-4e05-8072-a0fd077ba6c1" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment streamExecEnv = StreamExecutionEnvironment.getExecutionEnvironment();

DeserializationSchema<SomeObject> deserializer = (...);
SourceFunction<SomeObject> pubsubSource = PubSubSource.newBuilder()
                                                      .withDeserializationSchema(deserializer)
                                                      .withProjectName("project")
                                                      .withSubscriptionName("subscription")
                                                      .build();

streamExecEnv.addSource(pubsubSource);
```
{{< /tab >}}
{{< /tabs >}}

Currently the source functions [pulls](https://cloud.google.com/pubsub/docs/pull) messages from PubSub, [push endpoints](https://cloud.google.com/pubsub/docs/push) are not supported.

### PubSub Sink

The class `PubSubSink` has a builder to create PubSubSinks. `PubSubSink.newBuilder(...)`

This builder works in a similar way to the PubSubSource.

Example:

```java
DataStream<SomeObject> dataStream = (...);

SerializationSchema<SomeObject> serializationSchema = (...);
SinkFunction<SomeObject> pubsubSink = PubSubSink.newBuilder()
                                                .withSerializationSchema(serializationSchema)
                                                .withProjectName("project")
                                                .withTopicName("topic")
                                                .build()

dataStream.addSink(pubsubSink);
```

Note: This Sink has been deprecated in favor of the `PubSubSinkV2` which extends the new `SinkV2` API.


### PubSubSinkV2

Alternatively to the `PubSubSink` you can use the `PubSubSinkV2` which extends the new `SinkV2` API.
You can instantiate the `PubSubSinkV2` using the builder `PubSubSinkV2Builder` as follows:

```java
PubSubSinkV2Builder<String> pubSubSink =  PubSubSinkV2Builder.<String>builder()         
        .setProjectId("project-id")
        .setTopicId("topic-id")
        .setGcpPublisherConfig(gcpPublisherConfig)
        .setSerializationSchema(new SimpleStringSchema())
        .setMaximumInflightMessages(10)
        .setFailOnError(true)
        .build();
```
The `PubSubSinkV2` uses `com.google.cloud.pubsub.v1.Publisher` to publish messages to PubSub. 

The Sink is configured using `GcpPublisherConfig` which can be created using the builder `GcpPublisherConfigBuilder` as follows:

```java
GcpPublisherConfig gcpPublisherConfig = GcpPublisherConfigBuilder.builder()
        .setCredentialsProvider(credentialsProvider)
        .setBatchingSettings(BatchingSettings.newBuilder()
                .setDelayThreshold(Duration.ofMillis(10))
                .setElementCountThreshold(10L)
                .build())
        .setRetrySettings(RetrySettings.newBuilder()
                .setInitialRpcTimeout(Duration.ofSeconds(10))
                .setMaxRpcTimeout(Duration.ofSeconds(10))
                .setTotalTimeout(Duration.ofSeconds(10))
                .setInitialRetryDelay(Duration.ofSeconds(10))
                .setMaxRetryDelay(Duration.ofSeconds(10))
                .setRetryDelayMultiplier(1.0)
                .setRpcTimeoutMultiplier(1.0)
                .setMaxAttempts(10)
                .build())
        .setEnableCompression(true)
        .build();
```

The `PubSubSinkV2` does not buffer messages before sending them to PubSub. Instead, it uses the `Publisher`'s batching settings to batch messages before sending them to PubSub.

### Google Credentials

Google uses [Credentials](https://cloud.google.com/docs/authentication/production) to authenticate and authorize applications so that they can use Google Cloud Platform resources (such as PubSub).

Both builders allow you to provide these credentials but by default the connectors will look for an environment variable: [GOOGLE_APPLICATION_CREDENTIALS](https://cloud.google.com/docs/authentication/production#obtaining_and_providing_service_account_credentials_manually) which should point to a file containing the credentials.

If you want to provide Credentials manually, for instance if you read the Credentials yourself from an external system, you can use `PubSubSource.newBuilder(...).withCredentials(...)`.

### Integration testing

When running integration tests you might not want to connect to PubSub directly but use a docker container to read and write to. (See: [PubSub testing locally](https://cloud.google.com/pubsub/docs/emulator))

The following example shows how you would create a source to read messages from the emulator and send them back:

```java
String hostAndPort = "localhost:1234";
DeserializationSchema<SomeObject> deserializationSchema = (...);
SourceFunction<SomeObject> pubsubSource = PubSubSource.newBuilder()
                                                      .withDeserializationSchema(deserializationSchema)
                                                      .withProjectName("my-fake-project")
                                                      .withSubscriptionName("subscription")
                                                      .withPubSubSubscriberFactory(new PubSubSubscriberFactoryForEmulator(hostAndPort, "my-fake-project", "subscription", 10, Duration.ofSeconds(15), 100))
                                                      .build();
SerializationSchema<SomeObject> serializationSchema = (...);
SinkFunction<SomeObject> pubsubSink = PubSubSink.newBuilder()
                                                .withSerializationSchema(serializationSchema)
                                                .withProjectName("my-fake-project")
                                                .withTopicName("topic")
                                                .withHostAndPortForEmulator(hostAndPort)
                                                .build();

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.addSource(pubsubSource)
   .addSink(pubsubSink);
```

### At least once guarantee

#### SourceFunction

There are several reasons why a message might be send multiple times, such as failure scenarios on Google PubSub's side.

Another reason is when the acknowledgement deadline has passed. This is the time between receiving the message and acknowledging the message. The PubSubSource will only acknowledge a message on successful checkpoints to guarantee at-least-once. This does mean if the time between successful checkpoints is larger than the acknowledgment deadline of your subscription messages will most likely be processed multiple times.

For this reason it's recommended to have a (much) lower checkpoint interval than acknowledgement deadline.

See [PubSub](https://cloud.google.com/pubsub/docs/subscriber) for details on how to increase the acknowledgment deadline of your subscription.

Note: The metric `PubSubMessagesProcessedNotAcked` shows how many messages are waiting for the next checkpoint before they will be acknowledged.

#### SinkFunction

The sink function buffers messages that are to be send to PubSub for a short amount of time for performance reasons. Before each checkpoint this buffer is flushed and the checkpoint will not succeed unless the messages have been delivered to PubSub.

{{< top >}}
