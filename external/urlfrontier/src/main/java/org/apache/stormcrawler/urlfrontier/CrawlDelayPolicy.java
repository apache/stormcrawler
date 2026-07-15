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

package org.apache.stormcrawler.urlfrontier;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;

/**
 * Decides whether a queue-stream tuple carries a robots.txt crawl-delay worth forwarding to the
 * frontier via {@code setDelay}. The fetcher reports the delay (in seconds, {@code
 * robots.crawl.delay} metadata) only when it exceeds {@code fetcher.max.crawl.delay} and {@code
 * fetcher.max.crawl.delay.force} lets it keep fetching. When frontier pacing is explicitly enabled,
 * later hand-outs use the requested interval capped by {@code urlfrontier.backoff.max.secs}.
 *
 * <p>The signal is declarative — the last value wins, there is no escalation and no interaction
 * with {@link HostBackoff}'s blocks. Values are capped by {@code urlfrontier.backoff.max.secs}
 * (robots can request absurd delays; the parse-time cap is disabled in StormCrawler) and
 * deduplicated per host after a successful RPC. Only one request per host may be active; changes
 * received meanwhile are coalesced to the latest value, preventing older unary RPCs from
 * overwriting newer values out of order.
 *
 * <p>The decision runs on the bolt thread while gRPC callbacks may complete requests concurrently.
 * Per-key {@code ConcurrentMap.compute} operations serialize those transitions.
 */
final class CrawlDelayPolicy {

    /** Last delay applied successfully per host; expiry bounds the dedupe window and memory. */
    private final Cache<String, Integer> lastApplied;

    /** At most one unary RPC per host, plus the latest value observed while it is active. */
    private final ConcurrentMap<String, Flight> active = new ConcurrentHashMap<>();

    private final AtomicLong requestTokens = new AtomicLong();

    private final boolean enabled;

    private final long maxSecs;

    record DelayRequest(long token, String key, int delaySecs) {}

    private record Flight(DelayRequest request, Integer pendingLatest) {}

    CrawlDelayPolicy(Map<String, Object> conf) {
        this(conf, Ticker.systemTicker());
    }

    /** Visible for tests: a controllable ticker drives the dedupe window. */
    CrawlDelayPolicy(Map<String, Object> conf, Ticker ticker) {
        this.enabled =
                ConfUtils.getBoolean(
                                conf,
                                Constants.URLFRONTIER_ROBOTS_CRAWL_DELAY_ENABLED_KEY,
                                Constants.URLFRONTIER_ROBOTS_CRAWL_DELAY_ENABLED_DEFAULT)
                        && ConfUtils.getBoolean(conf, "fetcher.max.crawl.delay.force", false);
        this.maxSecs =
                Math.max(
                        1L,
                        ConfUtils.getLong(
                                conf,
                                Constants.URLFRONTIER_BACKOFF_MAX_KEY,
                                Constants.URLFRONTIER_BACKOFF_MAX_DEFAULT));
        long decaySecs =
                Math.max(
                        0L,
                        ConfUtils.getLong(
                                conf,
                                Constants.URLFRONTIER_BACKOFF_DECAY_KEY,
                                Constants.URLFRONTIER_BACKOFF_DECAY_DEFAULT));
        this.lastApplied =
                Caffeine.newBuilder()
                        .expireAfterWrite(decaySecs, TimeUnit.SECONDS)
                        .ticker(ticker)
                        .build();
    }

    /** Returns a request to start now, or empty when it is invalid, deduplicated or coalesced. */
    Optional<DelayRequest> requestFor(String key, Metadata metadata) {
        if (!enabled
                || key == null
                || metadata == null
                || HostBackoff.DEFAULT_QUEUE_KEY.equals(key)) {
            return Optional.empty();
        }
        final String value =
                metadata.getFirstValue(org.apache.stormcrawler.Constants.ROBOTS_CRAWL_DELAY_KEY);
        if (value == null) {
            return Optional.empty();
        }
        long secs;
        try {
            secs = Long.parseLong(value);
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        if (secs <= 0) {
            return Optional.empty();
        }
        final int delay = (int) Math.min(secs, Math.min(maxSecs, Integer.MAX_VALUE));
        AtomicReference<DelayRequest> next = new AtomicReference<>();
        active.compute(
                key,
                (ignored, flight) -> {
                    if (flight != null) {
                        return new Flight(flight.request(), delay);
                    }
                    Integer previous = lastApplied.getIfPresent(key);
                    if (previous != null && previous == delay) {
                        return null;
                    }
                    DelayRequest request =
                            new DelayRequest(requestTokens.incrementAndGet(), key, delay);
                    next.set(request);
                    return new Flight(request, null);
                });
        return Optional.ofNullable(next.get());
    }

    /** Completes one RPC and returns the latest coalesced value, if another RPC must follow. */
    Optional<DelayRequest> completed(DelayRequest completed, boolean success) {
        AtomicReference<DelayRequest> next = new AtomicReference<>();
        active.computeIfPresent(
                completed.key(),
                (key, flight) -> {
                    if (flight.request().token() != completed.token()) {
                        return flight;
                    }
                    if (success) {
                        lastApplied.put(key, completed.delaySecs());
                    } else {
                        // An error is ambiguous: the server may have applied the request before the
                        // response was lost. Forget the previous value so a later signal reasserts
                        // it instead of being incorrectly deduplicated.
                        lastApplied.invalidate(key);
                    }
                    Integer pending = flight.pendingLatest();
                    if (pending == null || (success && pending == completed.delaySecs())) {
                        return null;
                    }
                    DelayRequest request =
                            new DelayRequest(requestTokens.incrementAndGet(), key, pending);
                    next.set(request);
                    return new Flight(request, null);
                });
        return Optional.ofNullable(next.get());
    }

    /** Drops in-flight bookkeeping during bolt cleanup; terminal callbacks then become no-ops. */
    void clear() {
        active.clear();
    }
}
