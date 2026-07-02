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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;

class AsyncBulkProcessorTest {

    private static BulkOperation dummyOp() {
        return BulkOperation.of(b -> b.delete(d -> d.index("idx").id("1")));
    }

    private static BulkResponse emptyBulkResponse() {
        return new BulkResponse.Builder()
                .errors(false)
                .items(Collections.emptyList())
                .took(1)
                .build();
    }

    private static OpenSearchClient mockClient() throws IOException {
        OpenSearchClient client = mock(OpenSearchClient.class);
        when(client.bulk(any(BulkRequest.class))).thenReturn(emptyBulkResponse());
        return client;
    }

    /** Verify that a flush is triggered when the bulkActions threshold is reached. */
    @Test
    @Timeout(10)
    void flushAtBulkActionsThreshold() throws Exception {
        CountDownLatch afterBulkLatch = new CountDownLatch(1);
        AtomicInteger afterBulkCount = new AtomicInteger(0);

        AsyncBulkProcessor.Listener listener =
                new AsyncBulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {}

                    @Override
                    public void afterBulk(
                            long executionId, BulkRequest request, BulkResponse response) {
                        afterBulkCount.incrementAndGet();
                        afterBulkLatch.countDown();
                    }

                    @Override
                    public void afterBulk(
                            long executionId, BulkRequest request, Throwable failure) {}
                };

        OpenSearchClient client = mockClient();

        // bulkActions = 3, long flush interval so only threshold triggers
        AsyncBulkProcessor processor =
                new AsyncBulkProcessor.Builder(client, listener)
                        .setBulkActions(3)
                        .setFlushIntervalMillis(60_000)
                        .setConcurrentRequests(1)
                        .build();

        processor.add(dummyOp());
        processor.add(dummyOp());
        // third add should trigger flush
        processor.add(dummyOp());

        assertTrue(afterBulkLatch.await(5, TimeUnit.SECONDS), "afterBulk should have been called");
        assertEquals(1, afterBulkCount.get());

        processor.awaitClose(5, TimeUnit.SECONDS);
    }

    /** Verify that the timer-based flush fires even when bulkActions threshold is not reached. */
    @Test
    @Timeout(10)
    void timerBasedFlush() throws Exception {
        CountDownLatch afterBulkLatch = new CountDownLatch(1);

        AsyncBulkProcessor.Listener listener =
                new AsyncBulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {}

                    @Override
                    public void afterBulk(
                            long executionId, BulkRequest request, BulkResponse response) {
                        afterBulkLatch.countDown();
                    }

                    @Override
                    public void afterBulk(
                            long executionId, BulkRequest request, Throwable failure) {}
                };

        OpenSearchClient client = mockClient();

        // bulkActions very high, short flush interval
        AsyncBulkProcessor processor =
                new AsyncBulkProcessor.Builder(client, listener)
                        .setBulkActions(1000)
                        .setFlushIntervalMillis(200)
                        .setConcurrentRequests(1)
                        .build();

        processor.add(dummyOp());

        // should be flushed by timer within ~200ms
        assertTrue(
                afterBulkLatch.await(5, TimeUnit.SECONDS),
                "timer-based flush should have triggered");

        processor.awaitClose(5, TimeUnit.SECONDS);
    }

    /** Verify that concurrent requests are limited by the semaphore. */
    @Test
    @Timeout(10)
    void concurrentRequestLimiting() throws Exception {
        AtomicInteger concurrentCalls = new AtomicInteger(0);
        AtomicInteger maxConcurrent = new AtomicInteger(0);
        CountDownLatch allDone = new CountDownLatch(3);

        OpenSearchClient client = mock(OpenSearchClient.class);
        when(client.bulk(any(BulkRequest.class)))
                .thenAnswer(
                        invocation -> {
                            int current = concurrentCalls.incrementAndGet();
                            maxConcurrent.updateAndGet(prev -> Math.max(prev, current));
                            // simulate some work
                            Thread.sleep(200);
                            concurrentCalls.decrementAndGet();
                            allDone.countDown();
                            return emptyBulkResponse();
                        });

        AsyncBulkProcessor.Listener listener =
                new AsyncBulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {}

                    @Override
                    public void afterBulk(
                            long executionId, BulkRequest request, BulkResponse response) {}

                    @Override
                    public void afterBulk(
                            long executionId, BulkRequest request, Throwable failure) {}
                };

        // concurrentRequests = 1 means at most 1 in-flight request
        AsyncBulkProcessor processor =
                new AsyncBulkProcessor.Builder(client, listener)
                        .setBulkActions(1)
                        .setFlushIntervalMillis(60_000)
                        .setConcurrentRequests(1)
                        .build();

        // add 3 operations (each triggers flush since bulkActions=1)
        processor.add(dummyOp());
        processor.add(dummyOp());
        processor.add(dummyOp());

        assertTrue(allDone.await(5, TimeUnit.SECONDS));
        // with concurrentRequests=1, at most 1 bulk call should execute concurrently
        assertEquals(1, maxConcurrent.get());

        processor.awaitClose(5, TimeUnit.SECONDS);
    }

    /** Verify that awaitClose drains remaining buffered operations before returning. */
    @Test
    @Timeout(10)
    void awaitCloseDrainsPending() throws Exception {
        AtomicInteger totalBulkCalls = new AtomicInteger(0);

        AsyncBulkProcessor.Listener listener =
                new AsyncBulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {}

                    @Override
                    public void afterBulk(
                            long executionId, BulkRequest request, BulkResponse response) {
                        totalBulkCalls.incrementAndGet();
                    }

                    @Override
                    public void afterBulk(
                            long executionId, BulkRequest request, Throwable failure) {}
                };

        OpenSearchClient client = mockClient();

        // bulkActions very high so nothing auto-flushes, long interval
        AsyncBulkProcessor processor =
                new AsyncBulkProcessor.Builder(client, listener)
                        .setBulkActions(1000)
                        .setFlushIntervalMillis(60_000)
                        .setConcurrentRequests(1)
                        .build();

        // add some operations that won't auto-flush
        processor.add(dummyOp());
        processor.add(dummyOp());

        // awaitClose should drain the buffer
        boolean closed = processor.awaitClose(5, TimeUnit.SECONDS);
        assertTrue(closed, "awaitClose should return true");
        assertEquals(1, totalBulkCalls.get(), "buffered operations should have been flushed");
    }
}
