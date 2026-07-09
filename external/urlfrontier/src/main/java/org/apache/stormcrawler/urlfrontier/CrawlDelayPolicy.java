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
import java.util.concurrent.TimeUnit;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;

/**
 * Decides whether a queue-stream tuple carries a robots.txt crawl-delay worth forwarding to the
 * frontier via {@code setDelay}. The fetcher reports the delay (in seconds, {@code
 * robots.crawl.delay} metadata) only when it exceeds {@code fetcher.max.crawl.delay} and {@code
 * fetcher.max.crawl.delay.force} lets it keep fetching; the frontier then paces the queue at the
 * requested rate, restoring the compliance that force alone would violate.
 *
 * <p>The signal is declarative — the last value wins, there is no escalation and no interaction
 * with {@link HostBackoff}'s blocks. Values are capped by {@code urlfrontier.backoff.max.secs}
 * (robots can request absurd delays; the parse-time cap is disabled in StormCrawler) and
 * deduplicated per host: an unchanged value is re-sent at most once per {@code
 * urlfrontier.backoff.decay.secs} window, which also makes a lost fire-and-forget call converge.
 *
 * <p>Not thread-safe; a bolt task calls {@link #delaySecsFor} from a single thread.
 */
final class CrawlDelayPolicy {

    /** Last delay sent per host; expiry on write bounds both memory and RPC-loss staleness. */
    private final Cache<String, Integer> lastSent;

    private final long maxSecs;

    CrawlDelayPolicy(Map<String, Object> conf) {
        this(conf, Ticker.systemTicker());
    }

    /** Visible for tests: a controllable ticker drives the dedupe window. */
    CrawlDelayPolicy(Map<String, Object> conf, Ticker ticker) {
        this.maxSecs =
                ConfUtils.getLong(
                        conf,
                        Constants.URLFRONTIER_BACKOFF_MAX_KEY,
                        Constants.URLFRONTIER_BACKOFF_MAX_DEFAULT);
        long decaySecs =
                Math.max(
                        0L,
                        ConfUtils.getLong(
                                conf,
                                Constants.URLFRONTIER_BACKOFF_DECAY_KEY,
                                Constants.URLFRONTIER_BACKOFF_DECAY_DEFAULT));
        this.lastSent =
                Caffeine.newBuilder()
                        .expireAfterWrite(decaySecs, TimeUnit.SECONDS)
                        .ticker(ticker)
                        .build();
    }

    /**
     * @return the delay to forward to the frontier in seconds, or {@code -1} when the tuple carries
     *     none, the value is unchanged, or the shared {@code _DEFAULT_} queue is targeted
     */
    int delaySecsFor(String key, Metadata metadata) {
        if (metadata == null || HostBackoff.DEFAULT_QUEUE_KEY.equals(key)) {
            return -1;
        }
        final String value =
                metadata.getFirstValue(org.apache.stormcrawler.Constants.ROBOTS_CRAWL_DELAY_KEY);
        if (value == null) {
            return -1;
        }
        long secs;
        try {
            secs = Long.parseLong(value);
        } catch (NumberFormatException e) {
            return -1;
        }
        if (secs <= 0) {
            return -1;
        }
        final int delay = (int) Math.min(secs, Math.min(maxSecs, Integer.MAX_VALUE));
        final Integer previous = lastSent.getIfPresent(key);
        if (previous != null && previous == delay) {
            return -1;
        }
        lastSent.put(key, delay);
        return delay;
    }
}
