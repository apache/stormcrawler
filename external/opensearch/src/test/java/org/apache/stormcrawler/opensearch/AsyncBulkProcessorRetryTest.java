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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.opensearch.bolt.DeletionBolt;
import org.apache.stormcrawler.opensearch.persistence.StatusUpdaterBolt;
import org.apache.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import org.apache.stormcrawler.persistence.DefaultScheduler;
import org.apache.stormcrawler.persistence.Scheduler;
import org.apache.stormcrawler.persistence.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.OperationType;

/** Retry of items rejected with status 429. Runs without OpenSearch. */
class AsyncBulkProcessorRetryTest {

    /** Each request gets a future that the test completes. */
    private final List<BulkRequest> requests = new CopyOnWriteArrayList<>();

    private final List<CompletableFuture<BulkResponse>> futures = new CopyOnWriteArrayList<>();

    /** Retry tasks with their delays, run by the test. */
    private final List<Runnable> retries = new CopyOnWriteArrayList<>();

    private final List<Long> delays = new CopyOnWriteArrayList<>();

    private final List<Object> outcomes = new CopyOnWriteArrayList<>();

    private AsyncBulkProcessor.Listener delegate;

    private final AsyncBulkProcessor.Listener listener =
            new AsyncBulkProcessor.Listener() {
                @Override
                public void beforeBulk(long id, BulkRequest request) {
                    if (delegate != null) {
                        delegate.beforeBulk(id, request);
                    }
                }

                @Override
                public void afterBulk(long id, BulkRequest request, BulkResponse response) {
                    outcomes.add(response);
                    if (delegate != null) {
                        delegate.afterBulk(id, request, response);
                    }
                }

                @Override
                public void afterBulk(long id, BulkRequest request, Throwable failure) {
                    outcomes.add(failure);
                    if (delegate != null) {
                        delegate.afterBulk(id, request, failure);
                    }
                }
            };

    private final List<AsyncBulkProcessor> processors = new ArrayList<>();

    private final TestOutputCollector output = new TestOutputCollector();

    /**
     * Thrown by the next addToProcessor() of the bolt connection, the way a serialization error
     * would be.
     */
    private RuntimeException addFailure;

    @AfterEach
    void close() throws Exception {
        for (AsyncBulkProcessor p : processors) {
            p.awaitClose(100, TimeUnit.MILLISECONDS);
        }
    }

    private AsyncBulkProcessor processor(int bulkActions, long flushMillis, boolean manualRetries)
            throws IOException {
        OpenSearchAsyncClient client = mock(OpenSearchAsyncClient.class);
        AsyncBulkProcessorTest.stubMapper(client);
        when(client.bulk(any(BulkRequest.class)))
                .thenAnswer(
                        inv -> {
                            CompletableFuture<BulkResponse> future = new CompletableFuture<>();
                            requests.add(inv.getArgument(0));
                            futures.add(future);
                            return future;
                        });
        AsyncBulkProcessor.Builder builder =
                new AsyncBulkProcessor.Builder(client, listener)
                        .setBulkActions(bulkActions)
                        .setFlushIntervalMillis(flushMillis);
        if (manualRetries) {
            ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
            when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                    .thenAnswer(
                            inv -> {
                                retries.add(inv.getArgument(0));
                                delays.add(inv.getArgument(1));
                                return mock(ScheduledFuture.class);
                            });
            builder.retryScheduler(scheduler);
        }
        AsyncBulkProcessor p = builder.build();
        processors.add(p);
        return p;
    }

    private void reply(int call, int... statuses) {
        futures.get(call).complete(response(requests.get(call), statuses));
    }

    private void runRetry() {
        retries.remove(0).run();
    }

    private List<String> ids(int call) {
        return requests.get(call).operations().stream()
                .map(OpenSearchConnection::getBulkOperationId)
                .toList();
    }

    private static List<String> statuses(Object response) {
        return ((BulkResponse) response)
                .items().stream().map(i -> i.id() + ":" + i.status()).toList();
    }

    private static BulkOperation index(String id) {
        return BulkOperation.of(b -> b.index(i -> i.index("idx").id(id).document(Map.of("k", id))));
    }

    /** One item per operation, in request order. */
    private static BulkResponse response(BulkRequest request, int... statuses) {
        List<BulkResponseItem> items = new ArrayList<>();
        for (int i = 0; i < statuses.length; i++) {
            BulkOperation op = request.operations().get(i);
            int status = statuses[i];
            items.add(
                    BulkResponseItem.of(
                            b -> {
                                b.operationType(OperationType.valueOf(op._kind().name()))
                                        .index("idx")
                                        .id(OpenSearchConnection.getBulkOperationId(op))
                                        .status(status);
                                if (status >= 300) {
                                    b.error(e -> e.type("rejected_execution_exception"));
                                }
                                return b;
                            }));
        }
        boolean errors = Arrays.stream(statuses).anyMatch(s -> s >= 300);
        return BulkResponse.of(b -> b.took(1).errors(errors).items(items));
    }

    @Test
    void onlyItemsRejectedWith429AreResentAndTheMergedResponseKeepsRequestOrder()
            throws IOException {
        AsyncBulkProcessor p = processor(3, 60_000, true);
        p.add(index("a"));
        p.add(index("b"));
        p.add(index("c"));

        reply(0, 429, 409, 429);
        runRetry();
        assertEquals(List.of("a", "c"), ids(1));
        reply(1, 200, 429);
        runRetry();
        assertEquals(List.of("c"), ids(2));
        reply(2, 200);

        assertEquals(1, outcomes.size());
        assertEquals(List.of("a:200", "b:409", "c:200"), statuses(outcomes.get(0)));
        assertTrue(((BulkResponse) outcomes.get(0)).errors());
        assertEquals(List.of(50L, 60L), delays);
    }

    @Test
    void transportErrorDuringARetryFailsTheWholeBulk() throws IOException {
        AsyncBulkProcessor p = processor(2, 60_000, true);
        p.add(index("a"));
        p.add(index("b"));

        reply(0, 200, 429);
        runRetry();
        futures.get(1).completeExceptionally(new ConnectException("refused"));

        assertEquals(1, outcomes.size());
        assertInstanceOf(ConnectException.class, outcomes.get(0));
    }

    @Test
    @Timeout(10)
    void failureWhileHandlingTheResponseFailsTheBulkAndReleasesThePermit() throws IOException {
        AsyncBulkProcessor p = processor(1, 60_000, true);
        p.add(index("a"));
        BulkResponse broken = mock(BulkResponse.class);
        IllegalStateException failure = new IllegalStateException("broken response");
        when(broken.items()).thenThrow(failure);
        futures.get(0).complete(broken);
        p.add(index("b"));

        assertSame(failure, outcomes.get(0));
        assertEquals(2, requests.size());
    }

    /**
     * A bulk waiting for its retry keeps the only permit, so the timer flush of the next bulk
     * blocks the flush thread. The second retry is scheduled while that thread is blocked and still
     * runs, on the processor's own retry thread; the next bulk goes out after the listener.
     */
    @Test
    @Timeout(10)
    void permitIsHeldDuringTheBackoffWithoutBlockingTheRetry() throws Exception {
        AsyncBulkProcessor p = processor(2, 300, false);
        p.add(index("a1"));
        p.add(index("a2"));
        reply(0, 200, 429);
        await().atMost(Duration.ofSeconds(5)).until(() -> requests.size() == 2);
        p.add(index("b1"));
        await().atMost(Duration.ofSeconds(5))
                .until(AsyncBulkProcessorRetryTest::flushThreadWaitsForAPermit);

        reply(1, 429);
        await().atMost(Duration.ofSeconds(5)).until(() -> requests.size() == 3);
        assertEquals(List.of("a2"), ids(2));
        reply(2, 200);

        await().atMost(Duration.ofSeconds(5)).until(() -> requests.size() == 4);
        assertEquals(List.of("b1"), ids(3));
        assertEquals(List.of("a1:200", "a2:200"), statuses(outcomes.get(0)));
    }

    private static boolean flushThreadWaitsForAPermit() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(
                        t ->
                                t.getName().equals("AsyncBulkProcessor-flush")
                                        && t.getState() == Thread.State.WAITING);
    }

    @Test
    @Timeout(10)
    void closeWaitsForAPendingRetry() throws Exception {
        AsyncBulkProcessor p = processor(1, 60_000, false);
        p.add(index("a"));
        reply(0, 429);
        CompletableFuture.runAsync(
                () -> {
                    await().atMost(Duration.ofSeconds(5)).until(() -> requests.size() == 2);
                    reply(1, 200);
                });

        assertTrue(p.awaitClose(5, TimeUnit.SECONDS));
        assertEquals(List.of("a:200"), statuses(outcomes.get(0)));
    }

    private StatusUpdaterBolt bolt(int bulkActions) throws IOException {
        return prepared(new StatusUpdaterBolt(), bulkActions);
    }

    /**
     * The bolt prepared with the real processor and WaitAckCache. OpenSearchConnection is mocked
     * statically so that prepare() gets a connection which feeds the processor, and the listener
     * the bolt passes to it is captured as {@code delegate}, so that the outcomes reach the bolt.
     */
    private <B extends IBolt> B prepared(B bolt, int bulkActions) throws IOException {
        AsyncBulkProcessor p = processor(bulkActions, 60_000, true);
        OpenSearchConnection connection = mock(OpenSearchConnection.class);
        doAnswer(
                        inv -> {
                            if (addFailure != null) {
                                RuntimeException e = addFailure;
                                addFailure = null;
                                throw e;
                            }
                            p.add(inv.getArgument(0));
                            return null;
                        })
                .when(connection)
                .addToProcessor(any(BulkOperation.class));
        Map<String, Object> conf = new HashMap<>();
        conf.put(Scheduler.schedulerClassParamName, DefaultScheduler.class.getName());
        conf.put(
                AbstractStatusUpdaterBolt.cacheConfigParamName,
                "maximumSize=10000,expireAfterAccess=1h");
        try (MockedStatic<OpenSearchConnection> factory = mockStatic(OpenSearchConnection.class);
                MockedStatic<IndexCreation> ignored = mockStatic(IndexCreation.class)) {
            factory.when(() -> OpenSearchConnection.getBulkOperationId(any())).thenCallRealMethod();
            factory.when(() -> OpenSearchConnection.getConnection(any(), anyString(), any()))
                    .thenAnswer(
                            inv -> {
                                delegate = inv.getArgument(2);
                                return connection;
                            });
            bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        }
        return bolt;
    }

    private static Tuple tuple(String url) {
        return TestUtil.getMockedTestTuple(
                Map.of("url", url, "status", Status.FETCHED, "metadata", new Metadata()));
    }

    @Test
    void retried429IsAckedAfterSuccess() throws IOException {
        StatusUpdaterBolt bolt = bolt(1);
        Tuple t = tuple("https://a/");
        bolt.execute(t);

        reply(0, 429);
        assertTrue(output.getAckedTuples().isEmpty() && output.getFailedTuples().isEmpty());
        runRetry();
        reply(1, 200);

        assertEquals(List.of(t), output.getAckedTuples());
    }

    @Test
    void tupleFailsWhenRetriesRunOut() throws IOException {
        StatusUpdaterBolt bolt = bolt(1);
        Tuple t = tuple("https://a/");
        bolt.execute(t);

        reply(0, 429);
        for (int call = 1; call <= 8; call++) {
            runRetry();
            reply(call, 429);
        }

        assertEquals(List.of(50L, 60L, 80L, 150L, 280L, 580L, 1250L, 2740L), delays);
        assertEquals(List.of(t), output.getFailedTuples());
    }

    @Test
    void tupleFailedWhileAddingIsNotAckedByALaterWrite() throws IOException {
        StatusUpdaterBolt bolt = bolt(1);
        Tuple failed = tuple("https://a/");
        addFailure = new IllegalStateException("cannot serialize");
        bolt.execute(failed);
        Tuple next = tuple("https://a/");
        bolt.execute(next);

        reply(0, 200);

        assertEquals(List.of(failed), output.getFailedTuples());
        assertEquals(List.of(next), output.getAckedTuples());
    }

    @Test
    void deletionFailedWhileAddingIsNotAckedByALaterDeletion() throws IOException {
        DeletionBolt bolt = prepared(new DeletionBolt(), 1);
        Tuple failed = tuple("https://a/");
        addFailure = new IllegalStateException("cannot serialize");
        bolt.execute(failed);
        Tuple next = tuple("https://a/");
        bolt.execute(next);

        reply(0, 200);

        assertEquals(List.of(failed), output.getFailedTuples());
        assertEquals(List.of(next), output.getAckedTuples());
    }

    /**
     * Two tuples for one URL in one bulk: WaitAckCache acks both when one write succeeded, although
     * the second write was never stored.
     */
    @Test
    void duplicateIdIsAckedWhenOneWriteSucceeds() throws IOException {
        StatusUpdaterBolt bolt = bolt(2);
        bolt.execute(tuple("https://a/"));
        bolt.execute(tuple("https://a/"));

        reply(0, 200, 429);
        for (int call = 1; call <= 8; call++) {
            runRetry();
            assertEquals(1, requests.get(call).operations().size());
            reply(call, 429);
        }

        assertEquals(2, output.getAckedTuples().size());
    }
}
