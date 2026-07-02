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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.OperationType;
import org.slf4j.LoggerFactory;

class WaitAckCacheTest {

    private WaitAckCache cache;
    private List<Tuple> evicted;
    private List<Tuple> acked;
    private List<Tuple> failed;

    @BeforeEach
    void setUp() {
        evicted = new CopyOnWriteArrayList<>();
        acked = new ArrayList<>();
        failed = new ArrayList<>();
        cache = new WaitAckCache(LoggerFactory.getLogger(WaitAckCacheTest.class), evicted::add);
    }

    private Tuple mockTuple(String url) {
        Tuple t = mock(Tuple.class);
        when(t.getValueByField("url")).thenReturn(url);
        when(t.getStringByField("url")).thenReturn(url);
        return t;
    }

    private static BulkResponseItem successItem(String docId) {
        return BulkResponseItem.of(
                b -> b.id(docId).index("index").status(200).operationType(OperationType.Index));
    }

    private static BulkResponseItem failedItem(String docId, int status) {
        return BulkResponseItem.of(
                b ->
                        b.id(docId)
                                .index("index")
                                .status(status)
                                .operationType(OperationType.Index)
                                .error(
                                        ErrorCause.of(
                                                e -> e.type("test_error").reason("test failure"))));
    }

    private static BulkResponse bulkResponse(BulkResponseItem... items) {
        boolean hasErrors = false;
        for (BulkResponseItem item : items) {
            if (item.error() != null) {
                hasErrors = true;
                break;
            }
        }
        final boolean errors = hasErrors;
        return BulkResponse.of(b -> b.took(10).errors(errors).items(List.of(items)));
    }

    @Test
    void addAndContains() {
        Tuple t = mockTuple("http://example.com");
        assertFalse(cache.contains("doc1"));

        cache.addTuple("doc1", t);
        assertTrue(cache.contains("doc1"));
        assertEquals(1, cache.estimatedSize());
    }

    @Test
    void invalidateRemovesEntry() {
        Tuple t = mockTuple("http://example.com");
        cache.addTuple("doc1", t);
        assertTrue(cache.contains("doc1"));

        cache.invalidate("doc1");
        assertFalse(cache.contains("doc1"));
    }

    @Test
    void processBulkResponse_successfulItem_ackedViaTupleAction() {
        Tuple t = mockTuple("http://example.com");
        cache.addTuple("doc1", t);

        BulkResponse response = bulkResponse(successItem("doc1"));

        cache.processBulkResponse(
                response,
                1L,
                null,
                (id, tuple, selected) -> {
                    if (!selected.failed()) {
                        acked.add(tuple);
                    } else {
                        failed.add(tuple);
                    }
                });

        assertEquals(1, acked.size());
        assertEquals(0, failed.size());
        assertSame(t, acked.get(0));
        assertFalse(cache.contains("doc1"));
    }

    @Test
    void processBulkResponse_failedItem_failedViaTupleAction() {
        Tuple t = mockTuple("http://example.com");
        cache.addTuple("doc1", t);

        BulkResponse response = bulkResponse(failedItem("doc1", 500));

        cache.processBulkResponse(
                response,
                1L,
                null,
                (id, tuple, selected) -> {
                    if (!selected.failed()) {
                        acked.add(tuple);
                    } else {
                        failed.add(tuple);
                    }
                });

        assertEquals(0, acked.size());
        assertEquals(1, failed.size());
        assertSame(t, failed.get(0));
    }

    @Test
    void processBulkResponse_conflictIsNotAFailure() {
        Tuple t = mockTuple("http://example.com");
        cache.addTuple("doc1", t);

        ScopedCounter counter = mock(ScopedCounter.class);
        ScopedCounter.CountHandle handle = mock(ScopedCounter.CountHandle.class);
        when(counter.scope("doc_conflicts")).thenReturn(handle);

        BulkResponse response = bulkResponse(failedItem("doc1", 409));

        cache.processBulkResponse(
                response,
                1L,
                counter,
                (id, tuple, selected) -> {
                    if (!selected.failed()) {
                        acked.add(tuple);
                    } else {
                        failed.add(tuple);
                    }
                });

        assertEquals(1, acked.size());
        assertEquals(0, failed.size());
        verify(handle).incrBy(1);
    }

    @Test
    void processBulkResponse_multipleTuplesForSameDocId() {
        Tuple t1 = mockTuple("http://example.com/1");
        Tuple t2 = mockTuple("http://example.com/2");
        cache.addTuple("doc1", t1);
        cache.addTuple("doc1", t2);

        BulkResponse response = bulkResponse(successItem("doc1"));

        cache.processBulkResponse(response, 1L, null, (id, tuple, selected) -> acked.add(tuple));

        assertEquals(2, acked.size());
        assertTrue(acked.contains(t1));
        assertTrue(acked.contains(t2));
    }

    @Test
    void processBulkResponse_duplicateDocIdInBulk_prefersSuccess() {
        // https://github.com/apache/stormcrawler/issues/832
        Tuple t = mockTuple("http://example.com");
        cache.addTuple("doc1", t);

        BulkResponse response = bulkResponse(failedItem("doc1", 500), successItem("doc1"));

        cache.processBulkResponse(
                response,
                1L,
                null,
                (id, tuple, selected) -> {
                    if (!selected.failed()) {
                        acked.add(tuple);
                    } else {
                        failed.add(tuple);
                    }
                });

        assertEquals(1, acked.size());
        assertEquals(0, failed.size());
    }

    @Test
    void processFailedBulk_failsAllMatchingTuples() {
        Tuple t1 = mockTuple("http://example.com/1");
        Tuple t2 = mockTuple("http://example.com/2");
        cache.addTuple("doc1", t1);
        cache.addTuple("doc2", t2);

        BulkRequest request =
                BulkRequest.of(
                        b ->
                                b.operations(
                                        BulkOperation.of(
                                                o -> o.delete(d -> d.index("index").id("doc1"))),
                                        BulkOperation.of(
                                                o -> o.delete(d -> d.index("index").id("doc2")))));

        cache.processFailedBulk(request, 1L, new Exception("connection lost"), failed::add);

        assertEquals(2, failed.size());
        assertTrue(failed.contains(t1));
        assertTrue(failed.contains(t2));
        assertFalse(cache.contains("doc1"));
        assertFalse(cache.contains("doc2"));
    }

    @Test
    void processFailedBulk_ignoresMissingIds() {
        Tuple t = mockTuple("http://example.com");
        cache.addTuple("doc1", t);

        BulkRequest request =
                BulkRequest.of(
                        b ->
                                b.operations(
                                        BulkOperation.of(
                                                o ->
                                                        o.delete(
                                                                d ->
                                                                        d.index("index")
                                                                                .id(
                                                                                        "doc_unknown")))));

        cache.processFailedBulk(request, 1L, new Exception("test"), failed::add);

        assertEquals(0, failed.size());
        // doc1 should still be in cache since it wasn't in the failed request
        assertTrue(cache.contains("doc1"));
    }

    @Test
    void eviction_failsTuplesOnExpiry() {
        cache =
                new WaitAckCache(
                        "expireAfterWrite=1s",
                        LoggerFactory.getLogger(WaitAckCacheTest.class),
                        evicted::add);
        Tuple t = mockTuple("http://example.com");
        cache.addTuple("doc1", t);

        // Force cache maintenance after expiry by doing a contains() check
        // which accesses the cache and triggers Caffeine's cleanup
        await().atMost(5, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // contains() accesses the cache which triggers cleanup
                            cache.contains("doc1");
                            // also try adding and invalidating a dummy entry to force maintenance
                            Tuple dummy = mockTuple("http://dummy");
                            cache.addTuple("_probe_", dummy);
                            cache.invalidate("_probe_");
                            assertFalse(evicted.isEmpty(), "Eviction callback should have fired");
                        });

        assertTrue(evicted.contains(t));
    }

    @Test
    void processBulkResponse_multipleDocIds() {
        Tuple t1 = mockTuple("http://example.com/1");
        Tuple t2 = mockTuple("http://example.com/2");
        cache.addTuple("doc1", t1);
        cache.addTuple("doc2", t2);

        BulkResponse response = bulkResponse(successItem("doc1"), failedItem("doc2", 500));

        cache.processBulkResponse(
                response,
                1L,
                null,
                (id, tuple, selected) -> {
                    if (!selected.failed()) {
                        acked.add(tuple);
                    } else {
                        failed.add(tuple);
                    }
                });

        assertEquals(1, acked.size());
        assertSame(t1, acked.get(0));
        assertEquals(1, failed.size());
        assertSame(t2, failed.get(0));
    }
}
