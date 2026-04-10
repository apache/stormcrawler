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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Ticker;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
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

    private static ShardId shardId() {
        return new ShardId("index", "_na_", 0);
    }

    private static BulkItemResponse successItem(int itemId, String docId) {
        IndexResponse indexResponse = new IndexResponse(shardId(), docId, 1, 1, 1, true);
        return new BulkItemResponse(itemId, DocWriteRequest.OpType.INDEX, indexResponse);
    }

    private static BulkItemResponse failedItem(int itemId, String docId, RestStatus status) {
        BulkItemResponse.Failure failure =
                new BulkItemResponse.Failure("index", docId, new Exception("test failure"), status);
        return new BulkItemResponse(itemId, DocWriteRequest.OpType.INDEX, failure);
    }

    private static BulkResponse bulkResponse(BulkItemResponse... items) {
        return new BulkResponse(items, 10L);
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

        BulkResponse response = bulkResponse(successItem(0, "doc1"));

        cache.processBulkResponse(
                response,
                1L,
                null,
                (id, tuple, selected) -> {
                    if (!selected.failed) {
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

        BulkResponse response =
                bulkResponse(failedItem(0, "doc1", RestStatus.INTERNAL_SERVER_ERROR));

        cache.processBulkResponse(
                response,
                1L,
                null,
                (id, tuple, selected) -> {
                    if (!selected.failed) {
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

        ScopedCounter counter = scopeName -> incrementBy -> {};
        BulkResponse response = bulkResponse(failedItem(0, "doc1", RestStatus.CONFLICT));

        cache.processBulkResponse(
                response,
                1L,
                counter,
                (id, tuple, selected) -> {
                    if (!selected.failed) {
                        acked.add(tuple);
                    } else {
                        failed.add(tuple);
                    }
                });

        assertEquals(1, acked.size());
        assertEquals(0, failed.size());
    }

    @Test
    void processBulkResponse_multipleTuplesForSameDocId() {
        Tuple t1 = mockTuple("http://example.com/1");
        Tuple t2 = mockTuple("http://example.com/2");
        cache.addTuple("doc1", t1);
        cache.addTuple("doc1", t2);

        BulkResponse response = bulkResponse(successItem(0, "doc1"));

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

        BulkResponse response =
                bulkResponse(
                        failedItem(0, "doc1", RestStatus.INTERNAL_SERVER_ERROR),
                        successItem(1, "doc1"));

        cache.processBulkResponse(
                response,
                1L,
                null,
                (id, tuple, selected) -> {
                    if (!selected.failed) {
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

        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest("index", "doc1"));
        request.add(new DeleteRequest("index", "doc2"));

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

        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest("index", "doc_unknown"));

        cache.processFailedBulk(request, 1L, new Exception("test"), failed::add);

        assertEquals(0, failed.size());
        // doc1 should still be in cache since it wasn't in the failed request
        assertTrue(cache.contains("doc1"));
    }

    @Test
    void eviction_failsTuplesOnExpiry() {
        AtomicLong fakeTime = new AtomicLong(0);
        Ticker fakeTicker = fakeTime::get;
        cache =
                new WaitAckCache(
                        "expireAfterWrite=1s",
                        LoggerFactory.getLogger(WaitAckCacheTest.class),
                        evicted::add,
                        fakeTicker);
        Tuple t = mockTuple("http://example.com");
        cache.addTuple("doc1", t);

        // Advance past the 1s expiry
        fakeTime.set(TimeUnit.SECONDS.toNanos(2));

        // Access triggers expiration; cleanUp forces listener execution
        cache.contains("doc1");
        cache.cleanUp();

        assertFalse(evicted.isEmpty(), "Eviction callback should have fired");
        assertTrue(evicted.contains(t));
    }

    @Test
    void processBulkResponse_multipleDocIds() {
        Tuple t1 = mockTuple("http://example.com/1");
        Tuple t2 = mockTuple("http://example.com/2");
        cache.addTuple("doc1", t1);
        cache.addTuple("doc2", t2);

        BulkResponse response =
                bulkResponse(
                        successItem(0, "doc1"),
                        failedItem(1, "doc2", RestStatus.INTERNAL_SERVER_ERROR));

        cache.processBulkResponse(
                response,
                1L,
                null,
                (id, tuple, selected) -> {
                    if (!selected.failed) {
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

    @Test
    void shutdown_failsAllRemainingTuples() {
        Tuple t1 = mockTuple("http://example.com/1");
        Tuple t2 = mockTuple("http://example.com/2");
        cache.addTuple("doc1", t1);
        cache.addTuple("doc2", t2);

        cache.shutdown();

        assertEquals(2, evicted.size());
        assertTrue(evicted.contains(t1));
        assertTrue(evicted.contains(t2));
        assertFalse(cache.contains("doc1"));
        assertFalse(cache.contains("doc2"));
    }
}
