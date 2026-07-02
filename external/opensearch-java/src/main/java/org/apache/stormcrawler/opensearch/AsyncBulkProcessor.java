/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.opensearch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replacement for the legacy {@code org.opensearch.action.bulk.BulkProcessor} that works with the
 * new opensearch-java client. Accumulates {@link BulkOperation} instances and flushes them to
 * OpenSearch either when the configured number of actions is reached or when a periodic timer
 * fires.
 *
 * <p>Concurrency is controlled via a {@link Semaphore}: each in-flight bulk request acquires a
 * permit, which provides natural back-pressure towards the Storm topology when the cluster slows
 * down.
 */
public final class AsyncBulkProcessor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncBulkProcessor.class);

    /** Listener interface equivalent to the legacy {@code BulkProcessor.Listener}. */
    public interface Listener {
        void beforeBulk(long executionId, BulkRequest request);

        void afterBulk(long executionId, BulkRequest request, BulkResponse response);

        void afterBulk(long executionId, BulkRequest request, Throwable failure);
    }

    private final OpenSearchClient client;
    private final Listener listener;
    private final int bulkActions;
    private final int concurrentRequests;
    private final Semaphore concurrencyPermits;
    private final AtomicLong executionIdGen = new AtomicLong(0);

    private final ReentrantLock lock = new ReentrantLock();
    private List<BulkOperation> buffer;

    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> flushTask;

    /** Dedicated executor for bulk HTTP calls -- avoids starvation of ForkJoinPool.commonPool(). */
    private final ExecutorService bulkExecutor;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private AsyncBulkProcessor(Builder builder) {
        this.client = builder.client;
        this.listener = builder.listener;
        this.bulkActions = builder.bulkActions;
        this.concurrentRequests = builder.concurrentRequests;
        this.concurrencyPermits = new Semaphore(this.concurrentRequests);
        this.buffer = new ArrayList<>(bulkActions);

        this.bulkExecutor =
                new ThreadPoolExecutor(
                        1,
                        this.concurrentRequests,
                        60L,
                        TimeUnit.SECONDS,
                        new SynchronousQueue<>(),
                        r -> {
                            Thread t = new Thread(r, "AsyncBulkProcessor-bulk");
                            t.setDaemon(true);
                            return t;
                        },
                        new ThreadPoolExecutor.CallerRunsPolicy());

        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "AsyncBulkProcessor-flush");
                            t.setDaemon(true);
                            return t;
                        });
        this.flushTask =
                scheduler.scheduleWithFixedDelay(
                        this::flushIfNeeded,
                        builder.flushIntervalMillis,
                        builder.flushIntervalMillis,
                        TimeUnit.MILLISECONDS);
    }

    /** Adds a single bulk operation. Triggers a flush when {@code bulkActions} is reached. */
    public void add(BulkOperation operation) {
        if (closed.get()) {
            throw new IllegalStateException("BulkProcessor is closed");
        }
        List<BulkOperation> toFlush = null;
        lock.lock();
        try {
            buffer.add(operation);
            if (buffer.size() >= bulkActions) {
                toFlush = swapBuffer();
            }
        } finally {
            lock.unlock();
        }
        if (toFlush != null) {
            executeBulk(toFlush);
        }
    }

    /** Timer-triggered flush: only flushes if the buffer is non-empty. */
    private void flushIfNeeded() {
        List<BulkOperation> toFlush = null;
        lock.lock();
        try {
            if (!buffer.isEmpty()) {
                toFlush = swapBuffer();
            }
        } finally {
            lock.unlock();
        }
        if (toFlush != null) {
            executeBulk(toFlush);
        }
    }

    /**
     * Swaps the current buffer with a fresh one and returns the old buffer. Caller must hold {@link
     * #lock}.
     */
    private List<BulkOperation> swapBuffer() {
        List<BulkOperation> old = buffer;
        buffer = new ArrayList<>(bulkActions);
        return old;
    }

    /** Builds the request, acquires a concurrency permit, and executes asynchronously. */
    private void executeBulk(List<BulkOperation> operations) {
        final long executionId = executionIdGen.incrementAndGet();
        final BulkRequest request = new BulkRequest.Builder().operations(operations).build();

        try {
            concurrencyPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            listener.afterBulk(executionId, request, e);
            return;
        }

        try {
            listener.beforeBulk(executionId, request);
        } catch (Exception e) {
            LOG.warn("beforeBulk callback threw exception", e);
        }

        CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return client.bulk(request);
                            } catch (Exception e) {
                                throw new BulkExecutionException(e);
                            }
                        },
                        bulkExecutor)
                .whenComplete(
                        (response, throwable) -> {
                            concurrencyPermits.release();
                            try {
                                if (throwable != null) {
                                    Throwable cause =
                                            throwable instanceof BulkExecutionException
                                                    ? throwable.getCause()
                                                    : throwable;
                                    listener.afterBulk(executionId, request, cause);
                                } else {
                                    listener.afterBulk(executionId, request, response);
                                }
                            } catch (Exception e) {
                                LOG.warn("afterBulk callback threw exception", e);
                            }
                        });
    }

    /**
     * Drains pending operations and waits for all in-flight bulk requests to complete, up to the
     * given timeout. Equivalent to the legacy {@code BulkProcessor.awaitClose()}.
     *
     * @return {@code true} if all operations completed within the timeout
     */
    public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        if (!closed.compareAndSet(false, true)) {
            return true;
        }

        flushTask.cancel(false);
        scheduler.shutdown();

        // Flush any remaining buffered operations
        List<BulkOperation> remaining = null;
        lock.lock();
        try {
            if (!buffer.isEmpty()) {
                remaining = swapBuffer();
            }
        } finally {
            lock.unlock();
        }
        if (remaining != null) {
            executeBulk(remaining);
        }

        // Wait for all in-flight requests to finish by acquiring all permits
        boolean acquired = concurrencyPermits.tryAcquire(concurrentRequests, timeout, unit);
        if (acquired) {
            concurrencyPermits.release(concurrentRequests);
        }

        bulkExecutor.shutdown();
        bulkExecutor.awaitTermination(timeout, unit);

        return acquired;
    }

    @Override
    public void close() {
        try {
            awaitClose(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Builder for {@link AsyncBulkProcessor}. */
    public static final class Builder {
        private final OpenSearchClient client;
        private final Listener listener;
        private int bulkActions = 50;
        private long flushIntervalMillis = 5000;
        private int concurrentRequests = 1;

        public Builder(OpenSearchClient client, Listener listener) {
            this.client = client;
            this.listener = listener;
        }

        public Builder setBulkActions(int bulkActions) {
            this.bulkActions = bulkActions;
            return this;
        }

        public Builder setFlushIntervalMillis(long millis) {
            this.flushIntervalMillis = millis;
            return this;
        }

        public Builder setConcurrentRequests(int concurrentRequests) {
            this.concurrentRequests = Math.max(1, concurrentRequests);
            return this;
        }

        public AsyncBulkProcessor build() {
            return new AsyncBulkProcessor(this);
        }
    }

    /** Unchecked wrapper for checked exceptions thrown during bulk execution. */
    private static final class BulkExecutionException extends RuntimeException {
        BulkExecutionException(Throwable cause) {
            super(cause);
        }
    }
}
