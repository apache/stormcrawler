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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.jetbrains.annotations.Nullable;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.core.rest.RestStatus;
import org.slf4j.Logger;

/**
 * Thread-safe cache that tracks in-flight tuples awaiting bulk acknowledgment from OpenSearch.
 * Provides shared logic for processing bulk responses and failing tuples on error, used by
 * IndexerBolt, DeletionBolt, and StatusUpdaterBolt.
 */
public class WaitAckCache {

    /** Callback invoked for each tuple when processing a successful bulk response. */
    @FunctionalInterface
    public interface TupleAction {
        void handle(String id, Tuple tuple, BulkItemResponseToFailedFlag selected);
    }

    private final Cache<String, List<Tuple>> cache;
    private final java.util.concurrent.locks.ReentrantLock lock =
            new java.util.concurrent.locks.ReentrantLock(true);
    private final Logger log;

    /** Creates a cache with a fixed 60-second expiry. */
    public WaitAckCache(Logger log, Consumer<Tuple> onEviction) {
        this(Caffeine.newBuilder().expireAfterWrite(60, TimeUnit.SECONDS), log, onEviction);
    }

    /** Creates a cache from a Caffeine spec string (e.g. "expireAfterWrite=300s"). */
    public WaitAckCache(String cacheSpec, Logger log, Consumer<Tuple> onEviction) {
        this(Caffeine.from(cacheSpec), log, onEviction);
    }

    private WaitAckCache(Caffeine<Object, Object> builder, Logger log, Consumer<Tuple> onEviction) {
        this.log = log;
        this.cache =
                builder.<String, List<Tuple>>removalListener(
                                (String key, List<Tuple> value, RemovalCause cause) -> {
                                    if (!cause.wasEvicted()) {
                                        return;
                                    }
                                    if (value != null) {
                                        log.error(
                                                "Purged from waitAck {} with {} values",
                                                key,
                                                value.size());
                                        for (Tuple t : value) {
                                            onEviction.accept(t);
                                        }
                                    } else {
                                        log.error("Purged from waitAck {} with no values", key);
                                    }
                                })
                        .build();
    }

    /** Registers a gauge metric that reports the estimated cache size. */
    public void registerMetric(TopologyContext context, String name, int timeBucketSecs) {
        context.registerMetric(name, () -> cache.estimatedSize(), timeBucketSecs);
    }

    public long estimatedSize() {
        return cache.estimatedSize();
    }

    /** Adds a tuple to the cache under the given document ID, creating the list if needed. */
    public void addTuple(String docID, Tuple tuple) {
        lock.lock();
        try {
            List<Tuple> tt = cache.get(docID, k -> new LinkedList<>());
            tt.add(tuple);
            if (log.isDebugEnabled()) {
                String url = (String) tuple.getValueByField("url");
                log.debug("Added to waitAck {} with ID {} total {}", url, docID, tt.size());
            }
        } finally {
            lock.unlock();
        }
    }

    /** Returns true if the cache contains an entry for the given document ID. */
    public boolean contains(String docID) {
        lock.lock();
        try {
            return cache.getIfPresent(docID) != null;
        } finally {
            lock.unlock();
        }
    }

    /** Invalidates a single cache entry. */
    public void invalidate(String docID) {
        lock.lock();
        try {
            cache.invalidate(docID);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Processes a successful bulk response: classifies each item (conflict vs failure), retrieves
     * cached tuples, selects the best response per document ID, and invokes the action for each
     * tuple.
     *
     * @param conflictCounter optional metric counter; if non-null, increments "doc_conflicts" scope
     *     for each conflict
     */
    public void processBulkResponse(
            BulkResponse response,
            long executionId,
            @Nullable MultiCountMetric conflictCounter,
            TupleAction action) {

        var idsToBulkItems =
                Arrays.stream(response.getItems())
                        .map(
                                bir -> {
                                    BulkItemResponse.Failure f = bir.getFailure();
                                    boolean failed = false;
                                    if (f != null) {
                                        if (f.getStatus().equals(RestStatus.CONFLICT)) {
                                            if (conflictCounter != null) {
                                                conflictCounter.scope("doc_conflicts").incrBy(1);
                                            }
                                            log.debug("Doc conflict ID {}", bir.getId());
                                        } else {
                                            log.error(
                                                    "Bulk item failure ID {}: {}", bir.getId(), f);
                                            failed = true;
                                        }
                                    }
                                    return new BulkItemResponseToFailedFlag(bir, failed);
                                })
                        .collect(
                                // https://github.com/apache/stormcrawler/issues/832
                                Collectors.groupingBy(b -> b.id, Collectors.toUnmodifiableList()));

        Map<String, List<Tuple>> presentTuples;
        long estimatedSize;
        Set<String> debugInfo = null;
        lock.lock();
        try {
            presentTuples = cache.getAllPresent(idsToBulkItems.keySet());
            if (!presentTuples.isEmpty()) {
                cache.invalidateAll(presentTuples.keySet());
            }
            estimatedSize = cache.estimatedSize();
            if (log.isDebugEnabled() && estimatedSize > 0L) {
                debugInfo = new HashSet<>(cache.asMap().keySet());
            }
        } finally {
            lock.unlock();
        }

        int ackCount = 0;
        int failureCount = 0;

        for (var entry : presentTuples.entrySet()) {
            final var id = entry.getKey();
            final var tuples = entry.getValue();
            final var bulkItems = idsToBulkItems.get(id);

            BulkItemResponseToFailedFlag selected = selectBest(bulkItems, id);

            if (tuples != null) {
                log.debug("Found {} tuple(s) for ID {}", tuples.size(), id);
                for (Tuple t : tuples) {
                    if (selected.failed) {
                        failureCount++;
                    } else {
                        ackCount++;
                    }
                    action.handle(id, t, selected);
                }
            } else {
                log.warn("Could not find unacked tuples for {}", id);
            }
        }

        log.info(
                "Bulk response [{}] : items {}, waitAck {}, acked {}, failed {}",
                executionId,
                idsToBulkItems.size(),
                estimatedSize,
                ackCount,
                failureCount);

        if (debugInfo != null) {
            for (String k : debugInfo) {
                log.debug("Still in wait ack after bulk response [{}] => {}", executionId, k);
            }
        }
    }

    /** Processes a failed bulk request by failing all associated tuples. */
    public void processFailedBulk(
            BulkRequest request, long executionId, Throwable failure, Consumer<Tuple> failAction) {

        log.error("Exception with bulk {} - failing the whole lot ", executionId, failure);

        final var failedIds =
                request.requests().stream()
                        .map(DocWriteRequest::id)
                        .collect(Collectors.toUnmodifiableSet());

        Map<String, List<Tuple>> failedTupleLists;
        lock.lock();
        try {
            failedTupleLists = cache.getAllPresent(failedIds);
            if (!failedTupleLists.isEmpty()) {
                cache.invalidateAll(failedTupleLists.keySet());
            }
        } finally {
            lock.unlock();
        }

        for (var id : failedIds) {
            var tuples = failedTupleLists.get(id);
            if (tuples != null) {
                log.debug("Failed {} tuple(s) for ID {}", tuples.size(), id);
                for (Tuple t : tuples) {
                    failAction.accept(t);
                }
            } else {
                log.warn("Could not find unacked tuple for {}", id);
            }
        }
    }

    /**
     * Selects the best response when there are multiple bulk items for the same document ID.
     * Prefers non-failed responses; warns when there is a mix of success and failure.
     */
    private BulkItemResponseToFailedFlag selectBest(
            List<BulkItemResponseToFailedFlag> items, String id) {
        if (items.size() == 1) {
            return items.get(0);
        }

        BulkItemResponseToFailedFlag best = items.get(0);
        int failedCount = 0;
        for (var item : items) {
            if (item.failed) {
                failedCount++;
            } else {
                best = item;
            }
        }
        if (failedCount > 0 && failedCount < items.size()) {
            log.warn(
                    "The id {} would result in an ack and a failure."
                            + " Using only the ack for processing.",
                    id);
        }
        return best;
    }
}
