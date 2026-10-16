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

import com.google.common.io.CountingOutputStream;
import jakarta.json.stream.JsonGenerator;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replacement for the legacy {@code org.opensearch.action.bulk.BulkProcessor} that works with the
 * new opensearch-java client. Accumulates {@link BulkOperation} instances and flushes them to
 * OpenSearch when the configured number of actions is reached, when the buffered operations reach 5
 * MiB, or when a periodic timer fires.
 *
 * <p>Bulk requests are sent with the {@link OpenSearchAsyncClient}, so the calling thread never
 * waits for the HTTP round trip. Concurrency is controlled via a {@link Semaphore}: each in-flight
 * bulk request acquires a permit, which is only released once the listener has processed the
 * outcome. This provides natural back-pressure towards the Storm topology when the cluster slows
 * down.
 *
 * <p>Items rejected with status 429 are sent again up to 8 times, while the bulk keeps its permit.
 * The listener's {@code afterBulk} is called once per bulk, with the items of all attempts in
 * request order.
 */
public final class AsyncBulkProcessor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncBulkProcessor.class);

    private static final long[] RETRY_DELAYS_MILLIS = {50, 60, 80, 150, 280, 580, 1250, 2740};

    /** Flush threshold, in bytes of the serialized operations before compression. */
    static final long BULK_SIZE_BYTES = 5L * 1024 * 1024;

    /** Listener interface equivalent to the legacy {@code BulkProcessor.Listener}. */
    public interface Listener {
        void beforeBulk(long executionId, BulkRequest request);

        void afterBulk(long executionId, BulkRequest request, BulkResponse response);

        void afterBulk(long executionId, BulkRequest request, Throwable failure);
    }

    private final OpenSearchAsyncClient client;
    private final JsonpMapper mapper;
    private final Listener listener;
    private final int bulkActions;
    private final int concurrentRequests;
    private final Semaphore concurrencyPermits;
    private final AtomicLong executionIdGen = new AtomicLong(0);

    private final ReentrantLock lock = new ReentrantLock();
    private List<BulkOperation> buffer;
    private long bufferedBytes;

    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> flushTask;

    // separate from the flush thread, which can block on a permit that a retrying bulk holds
    private final ScheduledExecutorService retryScheduler;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private AsyncBulkProcessor(Builder builder) {
        this.client = builder.client;
        this.mapper = client._transport().jsonpMapper();
        this.listener = builder.listener;
        this.bulkActions = builder.bulkActions;
        this.concurrentRequests = builder.concurrentRequests;
        this.concurrencyPermits = new Semaphore(this.concurrentRequests);
        this.buffer = new ArrayList<>(bulkActions);

        this.scheduler = daemonScheduler("AsyncBulkProcessor-flush");
        this.retryScheduler =
                builder.retryScheduler != null
                        ? builder.retryScheduler
                        : daemonScheduler("AsyncBulkProcessor-retry");
        this.flushTask =
                scheduler.scheduleWithFixedDelay(
                        this::flushIfNeeded,
                        builder.flushIntervalMillis,
                        builder.flushIntervalMillis,
                        TimeUnit.MILLISECONDS);
    }

    private static ScheduledExecutorService daemonScheduler(String name) {
        return Executors.newSingleThreadScheduledExecutor(
                r -> {
                    Thread t = new Thread(r, name);
                    t.setDaemon(true);
                    return t;
                });
    }

    /**
     * Adds a single bulk operation. Triggers a flush when {@code bulkActions} or 5 MiB of buffered
     * operations is reached. The operation is serialized on the calling thread to count its size,
     * so a serialization error is thrown from this method.
     */
    public void add(BulkOperation operation) {
        if (closed.get()) {
            throw new IllegalStateException("BulkProcessor is closed");
        }
        final long size = serializedSize(mapper, operation);
        List<BulkOperation> toFlush = null;
        lock.lock();
        try {
            buffer.add(operation);
            bufferedBytes += size;
            if (buffer.size() >= bulkActions || bufferedBytes >= BULK_SIZE_BYTES) {
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
        bufferedBytes = 0;
        return old;
    }

    /**
     * Bytes the transport writes for the operation: each NDJSON line (action, then document or
     * update body) serialized with the transport's mapper, plus a newline.
     */
    static long serializedSize(JsonpMapper mapper, BulkOperation operation) {
        long size = 0;
        for (Iterator<?> it = operation._serializables(); it.hasNext(); ) {
            final CountingOutputStream line =
                    new CountingOutputStream(OutputStream.nullOutputStream());
            try (JsonGenerator generator = mapper.jsonProvider().createGenerator(line)) {
                mapper.serialize(it.next(), generator);
            }
            size += line.getCount() + 1;
        }
        return size;
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

        send(
                new Bulk(executionId, request),
                request,
                IntStream.range(0, operations.size()).toArray());
    }

    /**
     * Sends one attempt of the bulk. {@code positions[i]} is the position in the original request
     * of the i-th operation of {@code attempt}.
     */
    private void send(Bulk bulk, BulkRequest attempt, int[] positions) {
        CompletableFuture<BulkResponse> future;
        try {
            future = client.bulk(attempt);
        } catch (Exception e) {
            future = CompletableFuture.failedFuture(e);
        }
        future.whenComplete(
                (response, throwable) -> {
                    Throwable failure =
                            throwable instanceof CompletionException && throwable.getCause() != null
                                    ? throwable.getCause()
                                    : throwable;
                    BulkResponse result = null;
                    if (failure == null) {
                        try {
                            final int[] rejected = bulk.merge(response, positions);
                            if (rejected.length > 0 && bulk.retries < RETRY_DELAYS_MILLIS.length) {
                                retry(bulk, rejected);
                                return;
                            }
                            result = bulk.result(response);
                        } catch (RuntimeException e) {
                            failure = e;
                        }
                    }
                    finish(bulk, result, failure);
                });
    }

    private void retry(Bulk bulk, int[] positions) {
        final long delay = RETRY_DELAYS_MILLIS[bulk.retries++];
        LOG.debug(
                "Bulk [{}]: retrying {} items rejected with status 429 in {} ms",
                bulk.executionId,
                positions.length,
                delay);
        final List<BulkOperation> operations = new ArrayList<>(positions.length);
        for (int position : positions) {
            operations.add(bulk.request.operations().get(position));
        }
        final BulkRequest attempt = new BulkRequest.Builder().operations(operations).build();
        retryScheduler.schedule(() -> send(bulk, attempt, positions), delay, TimeUnit.MILLISECONDS);
    }

    // the permit is released only after the listener has run, so that the caller can't
    // start a new bulk request while the outcome of the previous one is still being processed
    private void finish(Bulk bulk, BulkResponse response, Throwable failure) {
        try {
            if (failure != null) {
                listener.afterBulk(bulk.executionId, bulk.request, failure);
            } else {
                listener.afterBulk(bulk.executionId, bulk.request, response);
            }
        } catch (Exception e) {
            LOG.warn("afterBulk callback threw exception", e);
        } finally {
            concurrencyPermits.release();
        }
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

        // Wait for all in-flight requests to finish, retries included, by acquiring all permits
        try {
            boolean acquired = concurrencyPermits.tryAcquire(concurrentRequests, timeout, unit);
            if (acquired) {
                concurrencyPermits.release(concurrentRequests);
            }
            return acquired;
        } finally {
            // retries still waiting after the timeout are dropped
            retryScheduler.shutdownNow();
        }
    }

    @Override
    public void close() {
        try {
            awaitClose(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** One bulk and the merged items of its attempts. */
    private static final class Bulk {
        final long executionId;
        final BulkRequest request;

        // items is set once a response has items to retry. The attempts of a bulk run one after
        // the other, so these fields need no lock.
        List<BulkResponseItem> items;
        long took;
        int retries;

        Bulk(long executionId, BulkRequest request) {
            this.executionId = executionId;
            this.request = request;
        }

        /** Merges the items of an attempt and returns the positions still rejected with 429. */
        int[] merge(BulkResponse response, int[] positions) {
            final List<BulkResponseItem> received = response.items();
            if (items == null && received.stream().noneMatch(i -> i.status() == 429)) {
                return new int[0];
            }
            if (received.size() != positions.length) {
                throw new IllegalStateException("Bulk response items do not match the request");
            }
            if (items == null) {
                items = new ArrayList<>(received);
            } else {
                for (int i = 0; i < positions.length; i++) {
                    items.set(positions[i], received.get(i));
                }
            }
            took += response.took();
            return IntStream.range(0, items.size())
                    .filter(i -> items.get(i).status() == 429)
                    .toArray();
        }

        /** The response for the listener: the last one, or the merged items after retries. */
        BulkResponse result(BulkResponse last) {
            if (items == null) {
                return last;
            }
            final boolean errors = items.stream().anyMatch(i -> i.error() != null);
            return BulkResponse.of(b -> b.took(took).errors(errors).items(items));
        }
    }

    /** Builder for {@link AsyncBulkProcessor}. */
    public static final class Builder {
        private final OpenSearchAsyncClient client;
        private final Listener listener;
        private int bulkActions = 50;
        private long flushIntervalMillis = 5000;
        private int concurrentRequests = 1;
        private ScheduledExecutorService retryScheduler;

        public Builder(OpenSearchAsyncClient client, Listener listener) {
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

        /** Test hook: schedules the retries on the given executor. */
        Builder retryScheduler(ScheduledExecutorService retryScheduler) {
            this.retryScheduler = retryScheduler;
            return this;
        }

        public AsyncBulkProcessor build() {
            return new AsyncBulkProcessor(this);
        }
    }
}
