package org.apache.flink.connector.gcp.pubsub.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;

/** Exception classifier to detect Fatal exceptions in PubSub publisher. */
@Internal
public class PubSubExceptionClassifiers {
    private static FatalExceptionClassifier getTopicNotFoundClassifier() {
        return new FatalExceptionClassifier(
                e ->
                        e instanceof io.grpc.StatusRuntimeException
                                && e.getMessage().contains("NOT_FOUND"),
                e -> new PubSubException("Topic not found", e));
    }

    private static FatalExceptionClassifier getProjectNotFoundClassifier() {
        return new FatalExceptionClassifier(
                e ->
                        e instanceof io.grpc.StatusRuntimeException
                                && e.getMessage()
                                        .contains("NOT_FOUND: Requested project not found"),
                e -> new PubSubException("Project not found", e));
    }

    static FatalExceptionClassifier getFatalExceptionClassifier() {
        return FatalExceptionClassifier.createChain(
                getProjectNotFoundClassifier(), getTopicNotFoundClassifier());
    }
}
