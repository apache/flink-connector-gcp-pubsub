package org.apache.flink.connector.gcp.pubsub.sink.util;

import org.apache.flink.connector.gcp.pubsub.sink.GcpWriterClient;
import org.apache.flink.util.concurrent.FutureUtils;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.StatusRuntimeException;

import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * A test implementation of {@link GcpWriterClient} that allows for simulating the delivery of
 * messages.
 */
public class TestGcpWriterClient implements GcpWriterClient<PubsubMessage> {

    private final Executor executor = Executors.newSingleThreadExecutor();
    private boolean isShutdown = false;

    private boolean isFlushed = false;

    private BlockingQueue<String> deliveredMessages = new ArrayBlockingQueue<>(100);

    private final ConcurrentHashMap<String, CountDownLatch> messageLatchMap =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, ApiFuture<String>> messageFutureMap =
            new ConcurrentHashMap<>();

    private final HashSet<String> previouslyFailedMessages = new HashSet<>();

    private Function<String, StatusRuntimeException> exceptionSupplier;

    @Override
    public ApiFuture<String> publish(PubsubMessage message) {
        String messageString = message.getData().toStringUtf8();
        messageLatchMap.put(messageString, new CountDownLatch(1));

        // if this is a retry succeed immediately
        if (previouslyFailedMessages.contains(messageString)) {
            deliveredMessages.add(messageString);
            return ApiFutures.immediateFuture("OK");
        }

        TestApiFuture future = new TestApiFuture(message, exceptionSupplier);
        messageFutureMap.put(messageString, future);
        return future;
    }

    @Override
    public void flush() {
        isFlushed = true;
        for (String message : messageLatchMap.keySet()) {
            messageLatchMap.get(message).countDown();
        }
        for (String message : messageFutureMap.keySet()) {
            awaitMessage(message);
        }
    }

    @Override
    public void shutdown() {
        isShutdown = true;
    }

    public void deliverMessage(String message) {
        messageLatchMap.get(message).countDown();
    }

    public void awaitMessage(String message) {
        if (!messageLatchMap.containsKey(message)) {
            return;
        }

        ApiFuture<String> future = messageFutureMap.get(message);
        try {
            future.get();
        } catch (InterruptedException | ExecutionException ignored) {
        }
    }

    public void setExceptionSupplier(Function<String, StatusRuntimeException> exceptionSupplier) {
        this.exceptionSupplier = exceptionSupplier;
    }

    public boolean failedInitially(String message) {
        return previouslyFailedMessages.contains(message);
    }

    public boolean isShutdown() {
        return isShutdown;
    }

    public boolean isFlushed() {
        return isFlushed;
    }

    public BlockingQueue<String> getDeliveredMessages() {
        return deliveredMessages;
    }

    /**
     * A test implementation of {@link ApiFuture} that allows for simulating the delivery of
     * messages.
     */
    public class TestApiFuture implements ApiFuture<String> {

        private final CompletableFuture<String> asyncOperation;

        public TestApiFuture(
                PubsubMessage message, Function<String, StatusRuntimeException> exceptionSupplier) {
            asyncOperation =
                    FutureUtils.supplyAsync(
                            () -> {
                                try {
                                    CountDownLatch latch =
                                            messageLatchMap.get(message.getData().toStringUtf8());
                                    if (latch != null) {
                                        latch.await();
                                    }

                                    if (message.getData().toStringUtf8().startsWith("fail")) {
                                        previouslyFailedMessages.add(
                                                message.getData().toStringUtf8());
                                        throw exceptionSupplier.apply(
                                                message.getData().toStringUtf8());
                                    }

                                    deliveredMessages.put(message.getData().toStringUtf8());
                                    return "Ok";
                                } catch (InterruptedException ignored) {
                                }
                                return null;
                            },
                            executor);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return asyncOperation.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return asyncOperation.isCancelled();
        }

        @Override
        public boolean isDone() {
            return asyncOperation.isDone();
        }

        @Override
        public String get() throws ExecutionException, InterruptedException {
            return asyncOperation.get();
        }

        @Override
        public String get(long timeout, TimeUnit unit)
                throws ExecutionException, InterruptedException, TimeoutException {
            return asyncOperation.get(timeout, unit);
        }

        @Override
        public void addListener(Runnable runnable, Executor executor) {
            asyncOperation.whenComplete(
                    (result, throwable) -> {
                        executor.execute(runnable);
                    });
        }
    }
}
