/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.connector.gcp.pubsub.source.reader;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.gcp.pubsub.source.split.PubSubSplit;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriber;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriberFactory;
import org.apache.flink.util.Collector;

import com.google.auth.Credentials;
import com.google.pubsub.v1.ReceivedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

/**
 * A {@link SplitReader} to read from a given {@link PubSubSubscriber}.
 *
 * @param <T> the type of the record.
 */
public class PubSubSplitReader<T> implements SplitReader<Tuple2<T, Long>, PubSubSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubSplitReader.class);
    private static final int RECEIVED_MESSAGE_QUEUE_MAX_RETRY_COUNT = 5;
    private static final int RECEIVED_MESSAGE_QUEUE_CAPACITY = 500000;
    private static final long RECEIVED_MESSAGE_QUEUE_RETRY_SLEEP_MILLIS = 1000;
    private final PubSubDeserializationSchema<T> deserializationSchema;
    private final PubSubSubscriberFactory pubSubSubscriberFactory;
    private final Credentials credentials;
    private volatile PubSubSubscriber subscriber;
    private final PubSubCollector collector;

    // Store the IDs of GCP Pub/Sub messages we have fetched & processed. Since the reader thread
    // processes messages and the fetcher thread acknowledges them, the thread-safe queue
    // decouples them.
    private final BlockingQueue<String> ackIdsQueue =
            new ArrayBlockingQueue<>(RECEIVED_MESSAGE_QUEUE_CAPACITY);
    private final Map<Long, List<String>> messageIdsToAcknowledge = new HashMap<>();

    /**
     * @param deserializationSchema a deserialization schema to apply to incoming message payloads.
     * @param pubSubSubscriberFactory a factory from which a new subscriber can be created from
     * @param credentials the credentials to use for creating a new subscriber
     */
    public PubSubSplitReader(
            PubSubDeserializationSchema deserializationSchema,
            PubSubSubscriberFactory pubSubSubscriberFactory,
            Credentials credentials) {

        this.deserializationSchema = deserializationSchema;
        this.pubSubSubscriberFactory = pubSubSubscriberFactory;
        this.credentials = credentials;
        this.collector = new PubSubCollector();
    }

    @Override
    public RecordsWithSplitIds<Tuple2<T, Long>> fetch() throws IOException {
        RecordsBySplits.Builder<Tuple2<T, Long>> recordsBySplits = new RecordsBySplits.Builder<>();
        if (subscriber == null) {
            synchronized (this) {
                if (subscriber == null) {
                    subscriber = pubSubSubscriberFactory.getSubscriber(credentials);
                }
            }
        }

        for (ReceivedMessage receivedMessage : subscriber.pull()) {
            try {
                // Deserialize messages into a collector so that logic in the user-provided
                // deserialization schema decides how to map GCP Pub/Sub messages to records in
                // Flink. This allows e.g. batching together multiple Flink records in a single GCP
                // Pub/Sub message.
                deserializationSchema.deserialize(receivedMessage.getMessage(), collector);
                collector
                        .getMessages()
                        .forEach(
                                message ->
                                        recordsBySplits.add(
                                                PubSubSplit.SPLIT_ID,
                                                new Tuple2<>(
                                                        message,
                                                        // A timestamp provided by GCP Pub/Sub
                                                        // indicating when the message was initially
                                                        // published
                                                        receivedMessage
                                                                .getMessage()
                                                                .getPublishTime()
                                                                .getSeconds())));
            } catch (Exception e) {
                throw new IOException("Failed to deserialize received message due to", e);
            } finally {
                collector.reset();
            }

            enqueueAcknowledgementId(receivedMessage.getAckId());
        }

        return recordsBySplits.build();
    }

    /**
     * Enqueue an acknowledgment ID to be acknowledged towards GCP Pub/Sub with retries.
     *
     * @param ackId the ID of the message to acknowledge
     */
    public void enqueueAcknowledgementId(String ackId) {
        int retryCount = 0;

        while (retryCount < RECEIVED_MESSAGE_QUEUE_MAX_RETRY_COUNT) {
            boolean enqueued = ackIdsQueue.offer(ackId);
            if (!enqueued) {
                retryCount++;
                try {
                    Thread.sleep(RECEIVED_MESSAGE_QUEUE_RETRY_SLEEP_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.error("Thread interrupted while waiting to enqueue acknowledgment ID.", e);
                    return;
                }
            } else {
                return;
            }
        }

        LOG.warn(
                "Queue is full. Unable to enqueue acknowledgment ID after "
                        + RECEIVED_MESSAGE_QUEUE_MAX_RETRY_COUNT
                        + " retries.");
    }

    @Override
    public void handleSplitsChanges(SplitsChange<PubSubSplit> splitsChanges) {}

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (subscriber != null) {
            subscriber.close();
        }
    }

    private class PubSubCollector implements Collector<T> {
        private final List<T> messages = new ArrayList<>();

        @Override
        public void collect(T message) {
            messages.add(message);
        }

        @Override
        public void close() {}

        private List<T> getMessages() {
            return messages;
        }

        private void reset() {
            messages.clear();
        }
    }

    /**
     * Prepare for acknowledging messages received since the last checkpoint by draining the {@link
     * #ackIdsQueue} into {@link #messageIdsToAcknowledge}.
     *
     * <p>Calling this method is enqueued by the {@link PubSubSourceFetcherManager} to snapshot
     * state before a checkpoint.
     *
     * @param checkpointId the ID of the checkpoint for which to prepare for acknowledging messages
     */
    public void prepareForAcknowledgement(long checkpointId) {
        List<String> ackIds = new ArrayList<>();
        ackIdsQueue.drainTo(ackIds);
        messageIdsToAcknowledge.put(checkpointId, ackIds);
    }

    /**
     * Acknowledge the reception of messages towards GCP Pub/Sub since the last checkpoint. If a
     * received message is not acknowledged before the subscription's acknowledgment timeout, GCP
     * Pub/Sub will attempt to deliver it again.
     *
     * <p>Calling this method is enqueued by the {@link PubSubSourceFetcherManager} on checkpoint.
     *
     * @param checkpointId the ID of the checkpoint for which to acknowledge messages
     */
    void acknowledgeMessages(long checkpointId) throws IOException {
        if (subscriber == null) {
            synchronized (this) {
                if (subscriber == null) {
                    subscriber = pubSubSubscriberFactory.getSubscriber(credentials);
                }
            }
        }

        if (!messageIdsToAcknowledge.containsKey(checkpointId)) {
            LOG.error(
                    "Checkpoint {} not found in set of in-flight checkpoints {}.",
                    checkpointId,
                    messageIdsToAcknowledge.keySet().stream()
                            .map(String::valueOf)
                            .collect(Collectors.joining(",")));
            return;
        }

        List<String> messageIdsForCheckpoint = messageIdsToAcknowledge.remove(checkpointId);
        if (!messageIdsForCheckpoint.isEmpty()) {
            LOG.debug(
                    "Acknowledging {} messages for checkpoint {}.",
                    messageIdsForCheckpoint.size(),
                    checkpointId);
            subscriber.acknowledge(messageIdsForCheckpoint);
        } else {
            LOG.debug("No messages to acknowledge for checkpoint {}.", checkpointId);
        }

        // Handle the case where a checkpoint is aborted and the messages for that checkpoint are
        // never acknowledged. Here, we log any remaining checkpointIds and clear them. This relies
        // on GCP Pub/Sub to redeliver the unacked messages.
        if (!messageIdsToAcknowledge.isEmpty()) {
            // Loop through any remaining checkpointIds in messageIdsToAcknowledge, and then clear
            // them.
            for (Map.Entry<Long, List<String>> entry : messageIdsToAcknowledge.entrySet()) {
                LOG.warn(
                        "Checkpoint {} was not acknowledged - clearing {} unacked messages.",
                        entry.getKey(),
                        entry.getValue().size());
            }
            messageIdsToAcknowledge.clear();
        }
    }
}
