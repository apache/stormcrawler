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

package org.apache.stormcrawler.persistence.urlbuffer;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.collect.EvictingQueue;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.util.ConfUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Checks how long the last N URLs took to work out whether a queue should release a URL. */
public class SchedulingURLBuffer extends AbstractURLBuffer
        implements RemovalListener<String, Object[]> {

    static final Logger LOG = LoggerFactory.getLogger(SchedulingURLBuffer.class);

    public static final String MAXTIMEPARAM = "priority.buffer.max.time.msec";

    private int maxTimeMilliSec = 30000;

    // TODO: make it configurable
    private int historySize = 5;

    // keeps track of the URL having been sent
    private Cache<String, Object[]> unacked;

    private Cache<String, Queue<Long>> timings;

    private Cache<String, Instant> lastReleased;

    public void configure(Map<String, Object> stormConf) {
        super.configure(stormConf);
        maxTimeMilliSec = ConfUtils.getInt(stormConf, MAXTIMEPARAM, maxTimeMilliSec);
        unacked =
                Caffeine.newBuilder()
                        .expireAfterWrite(maxTimeMilliSec, TimeUnit.MILLISECONDS)
                        .removalListener(this)
                        .build();
        timings = Caffeine.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();
        lastReleased = Caffeine.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();
    }

    /**
     * Retrieves the next available URL, guarantees that the URLs are always perfectly shuffled.
     *
     * @return null if no entries are available
     */
    public synchronized Values next() {

        do {
            Iterator<Entry<String, Queue<URLMetadata>>> i = queues.entrySet().iterator();

            if (!i.hasNext()) {
                LOG.trace("Empty iterator");
                return null;
            }

            Map.Entry<String, Queue<URLMetadata>> nextEntry = i.next();

            Queue<URLMetadata> queue = nextEntry.getValue();
            String queueName = nextEntry.getKey();

            // remove the entry, gets added back later
            i.remove();

            LOG.trace("Next queue {}", queueName);

            URLMetadata item = null;

            // is this queue ready to be processed?
            if (canRelease(queueName)) {
                // try the first element
                item = queue.poll();
                LOG.trace("Item {}", item.url);
            } else {
                LOG.trace("Queue {} not ready to release yet", queueName);
            }

            // any left? add to the end of the iterator
            if (!queue.isEmpty()) {
                LOG.debug("Adding to the back of the queue {}", queueName);
                queues.put(queueName, queue);
            } else {
                // notify that the queue is empty
                if (listener != null) {
                    listener.emptyQueue(queueName);
                }
            }

            if (item != null) {
                lastReleased.put(queueName, Instant.now());
                unacked.put(item.url, new Object[] {Instant.now(), queueName});
                // remove it from the list of URLs in the queue
                inBuffer.remove(item.url);
                return new Values(item.url, item.metadata);
            }
        } while (!queues.isEmpty());
        return null;
    }

    private boolean canRelease(String queueName) {
        // return true if enough time has expired since the previous release
        // given the past performance of the last N urls

        Queue<Long> times = timings.getIfPresent(queueName);
        if (times == null) {
            return true;
        }

        // not enough history yet? just say yes
        if (times.size() < historySize) {
            return true;
        }

        // get the average duration over the recent history
        long totalMsec = 0L;
        for (Long t : times) {
            totalMsec += t;
        }
        long average = totalMsec / historySize;

        LOG.trace("Average for {}: {} msec", queueName, average);

        Instant lastRelease = lastReleased.getIfPresent(queueName);
        if (lastRelease == null) {
            // removed? bit unlikely but nevermind
            return true;
        }

        // check that enough time has elapsed
        // since the previous release from this queue
        return lastRelease.plusMillis(average).isBefore(Instant.now());
    }

    public void acked(String url) {
        // get notified that the URL has been acked
        // use that to compute how long it took
        Object[] cached = (Object[]) unacked.getIfPresent(url);
        // has already been discarded - its timing set to max
        if (cached == null) {
            return;
        }

        Instant t = (Instant) cached[0];
        String key = (String) cached[1];

        long tookmsec = Instant.now().toEpochMilli() - t.toEpochMilli();

        LOG.trace("Adding new timing for {}: {} msec - {}", key, tookmsec, url);

        // add the timing for the queue
        addTiming(tookmsec, key);
    }

    void addTiming(long t, String queueName) {
        Queue<Long> times = timings.get(queueName, k -> EvictingQueue.create(historySize));
        times.add(t);
    }

    @Override
    public void onRemoval(
            @Nullable String key, Object @Nullable [] value, @NotNull RemovalCause cause) {
        addTiming(maxTimeMilliSec, key);
    }
}
