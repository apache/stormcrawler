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

package org.apache.stormcrawler.fetcher;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import org.apache.storm.Config;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.bolt.FetcherBolt;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.LoggerFactory;

/**
 * Convenience class - a collection of queues that keeps track of the total number of items, and
 * provides items eligible for fetching from any queue.
 *
 * <p>Queues are kept in a {@link ConcurrentHashMap} and the ones which may have an item ready are
 * referenced from a {@link DelayQueue} ordered by their next fetch time: taking an item is O(log n)
 * and does not require a global lock, so the executor thread adding URLs is never blocked by the
 * fetcher threads.
 *
 * <p>For internal use only. Not part of StormCrawler's public API.
 */
public final class FetchItemQueues {
    // the bolt's category: log configurations for FetcherBolt keep covering these lines
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FetcherBolt.class);

    /** Key name of the custom crawl delay for a queue that may be present in the metadata. */
    private static final String CRAWL_DELAY_KEY_NAME = "crawl.delay";

    /**
     * Key name of the custom crawl delay for a queue that may be present in the metadata when
     * multi-threading is allowed for a queue.
     */
    private static final String CRAWL_MIN_DELAY_KEY_NAME = "crawl.min.delay";

    /** Key name of the custom max number of threads that may be present in the metadata. */
    private static final String CRAWL_MAX_THREAD_KEY_NAME = "max.threads.queue";

    final Map<String, FetchItemQueue> queues = new ConcurrentHashMap<>();

    private final DelayQueue<QueueTicket> ready = new DelayQueue<>();

    AtomicInteger inQueues = new AtomicInteger(0);

    final int defaultMaxThread;
    final long crawlDelay;
    final long minCrawlDelay;

    /** Cap of the backoff applied to a queue whose fetches find every helper thread busy. */
    final long maxBackoff;

    int maxQueueSize;

    final Config conf;

    static final String QUEUE_MODE_HOST = "byHost";
    static final String QUEUE_MODE_DOMAIN = "byDomain";
    static final String QUEUE_MODE_IP = "byIP";

    String queueMode;

    final Map<Pattern, Integer> customMaxThreads = new HashMap<>();

    public FetchItemQueues(Config conf) {
        this.conf = conf;
        this.defaultMaxThread = ConfUtils.getInt(conf, "fetcher.threads.per.queue", 1);
        queueMode = ConfUtils.getString(conf, "fetcher.queue.mode", QUEUE_MODE_HOST);
        // check that the mode is known
        if (!queueMode.equals(QUEUE_MODE_IP)
                && !queueMode.equals(QUEUE_MODE_DOMAIN)
                && !queueMode.equals(QUEUE_MODE_HOST)) {
            LOG.error("Unknown partition mode : {} - forcing to byHost", queueMode);
            queueMode = QUEUE_MODE_HOST;
        }
        LOG.info("Using queue mode : {}", queueMode);

        this.crawlDelay = (long) (ConfUtils.getFloat(conf, "fetcher.server.delay", 1.0f) * 1000);
        this.minCrawlDelay =
                (long) (ConfUtils.getFloat(conf, "fetcher.server.min.delay", 0.0f) * 1000);
        this.maxQueueSize = ConfUtils.getInt(conf, "fetcher.max.queue.size", -1);
        if (this.maxQueueSize == -1) {
            this.maxQueueSize = Integer.MAX_VALUE;
        }
        // a queue is never put off for longer than the longest politeness delay accepted,
        // or the default one when any delay is accepted (negative value)
        final int maxCrawlDelaySecs = ConfUtils.getInt(conf, "fetcher.max.crawl.delay", 30);
        this.maxBackoff = (maxCrawlDelaySecs < 0 ? 30 : maxCrawlDelaySecs) * 1000L;

        // order is not guaranteed
        for (Entry<String, Object> e : conf.entrySet()) {
            String key = e.getKey();
            if (!key.startsWith("fetcher.maxThreads.")) {
                continue;
            }
            Pattern patt = Pattern.compile(key.substring("fetcher.maxThreads.".length()));
            customMaxThreads.put(patt, ((Number) e.getValue()).intValue());
        }
    }

    /**
     * Adds item to the queue.
     *
     * @return true if the URL has been added, false otherwise.
     */
    public boolean addFetchItem(URL u, String url, Tuple input) {
        // built outside any lock: in byIP mode this resolves the hostname
        final FetchItem it = FetchItem.create(u, url, input, queueMode);
        final Metadata metadata = (Metadata) input.getValueByField("metadata");
        while (true) {
            FetchItemQueue fiq = getFetchItemQueue(it.queueId(), metadata);
            synchronized (fiq) {
                if (fiq.removed) {
                    // reaped concurrently: get a fresh one
                    continue;
                }
                if (!fiq.offer(it)) {
                    return false;
                }
            }
            inQueues.incrementAndGet();
            schedule(fiq, fiq.getNextFetchTime());
            LOG.debug("{} added to queue {}", url, it.queueId());
            return true;
        }
    }

    public void finishFetchItem(FetchItem it, boolean asap) {
        FetchItemQueue fiq = queues.get(it.queueId());
        if (fiq == null) {
            LOG.warn("Attempting to finish item from unknown queue: {}", it.queueId());
            return;
        }
        fiq.finish(asap);
        rescheduleOrReap(fiq);
    }

    /**
     * Releases the slot of an item whose fetch found every helper thread busy and backs the queue
     * off, see {@link FetchItemQueue#finishSaturated}.
     *
     * @return the delay applied to the queue in milliseconds, -1 if the queue is unknown
     */
    public long backOffFetchItem(FetchItem it) {
        FetchItemQueue fiq = queues.get(it.queueId());
        if (fiq == null) {
            LOG.warn("Attempting to back off item from unknown queue: {}", it.queueId());
            return -1;
        }
        long delay = fiq.finishSaturated(maxBackoff);
        rescheduleOrReap(fiq);
        return delay;
    }

    private void rescheduleOrReap(FetchItemQueue fiq) {
        if (fiq.queue.isEmpty()) {
            reapIfEmpty(fiq);
        } else {
            schedule(fiq, fiq.getNextFetchTime());
        }
    }

    /** Puts a ticket for the queue in the ready queue, unless one is already there. */
    private void schedule(FetchItemQueue fiq, long time) {
        if (fiq.scheduled.compareAndSet(false, true)) {
            ready.add(new QueueTicket(fiq, time));
        }
    }

    /** Removes the queue from the map if it holds nothing and nothing is in progress. */
    private void reapIfEmpty(FetchItemQueue fiq) {
        synchronized (fiq) {
            if (fiq.queue.isEmpty() && fiq.getInProgressSize() == 0 && !fiq.removed) {
                if (queues.remove(fiq.id, fiq)) {
                    fiq.removed = true;
                }
            }
        }
    }

    public FetchItemQueue getFetchItemQueue(String id, Metadata metadata) {
        long delay = crawlDelay;
        long minDelay = minCrawlDelay;

        if (metadata != null) {
            // custom crawl delay from metadata?
            String v = metadata.getFirstValue(CRAWL_DELAY_KEY_NAME);
            if (v != null) {
                try {
                    delay = Long.parseLong(v);
                } catch (NumberFormatException e) {
                    LOG.warn(
                            "Invalid crawl delay value '{}' in metadata for queue '{}', using"
                                    + " default.",
                            v,
                            id);
                }
            }
            // custom min crawl delay from metadata?
            v = metadata.getFirstValue(CRAWL_MIN_DELAY_KEY_NAME);
            if (v != null) {
                try {
                    minDelay = Long.parseLong(v);
                } catch (NumberFormatException e) {
                    LOG.warn(
                            "Invalid min crawl delay value '{}' in metadata for queue '{}',"
                                    + " using default.",
                            v,
                            id);
                }
            }
        }

        final long queueDelay = delay;
        final long queueMinDelay = minDelay;

        FetchItemQueue fiq =
                queues.computeIfAbsent(
                        id,
                        k -> {
                            int threadVal = defaultMaxThread;
                            // custom maxThread value?
                            for (Entry<Pattern, Integer> p : customMaxThreads.entrySet()) {
                                if (p.getKey().matcher(k).matches()) {
                                    threadVal = p.getValue();
                                    break;
                                }
                            }

                            // overridden at URL level
                            // custom thread number from metadata?
                            if (metadata != null) {
                                final String val =
                                        metadata.getFirstValue(CRAWL_MAX_THREAD_KEY_NAME);
                                if (val != null) {
                                    try {
                                        threadVal = Integer.parseInt(val);
                                    } catch (NumberFormatException e) {
                                        LOG.warn(
                                                "Invalid max threads value '{}' in metadata for"
                                                        + " queue '{}', using default.",
                                                val,
                                                k);
                                    }
                                }
                            }

                            return new FetchItemQueue(
                                    k, threadVal, queueDelay, queueMinDelay, maxQueueSize);
                        });

        // in cases where we have different pages with the same key that will fall in the same
        // queue, each one with a custom min crawl delay, we take the less aggressive. Atomic
        // max: this runs without a lock and from any fetcher thread as well as the executor
        fiq.minCrawlDelay.accumulateAndGet(minDelay, Math::max);
        // same for the normal delay
        fiq.crawlDelay.accumulateAndGet(delay, Math::max);
        return fiq;
    }

    /**
     * Returns an item from a queue whose crawl delay has elapsed and which has a free slot, or null
     * if there is none right now.
     */
    public FetchItem getFetchItem() {
        // bounded so that a burst of stale tickets can not keep a thread busy for long
        for (int attempt = 0; attempt < 1000; attempt++) {
            final QueueTicket ticket = ready.poll();
            if (ticket == null) {
                // nothing is due: the head of the heap is the earliest queue
                return null;
            }
            final FetchItemQueue fiq = ticket.fiq();
            final long now = System.currentTimeMillis();
            if (!fiq.isReady(now)) {
                // the delay was extended after the ticket was issued: re-issue it at the new
                // time (in the future, so it cannot come straight back) and look at the next
                // ticket, which may well be due
                ready.add(new QueueTicket(fiq, fiq.getNextFetchTime()));
                continue;
            }
            if (!fiq.hasFreeSlot()) {
                fiq.scheduled.set(false);
                // a fetch may have finished between the check and the clearing of the
                // flag, in which case its schedule() found the flag still set: re-check
                if (fiq.hasFreeSlot() && !fiq.queue.isEmpty()) {
                    schedule(fiq, fiq.getNextFetchTime());
                }
                continue;
            }
            final FetchItem it = fiq.poll();
            fiq.scheduled.set(false);
            if (it == null) {
                // either the queue is empty or the last slot was taken concurrently
                reapIfEmpty(fiq);
                if (fiq.hasFreeSlot() && !fiq.queue.isEmpty()) {
                    // lost a race with a concurrent add or finish: re-issue the ticket
                    schedule(fiq, fiq.getNextFetchTime());
                }
                continue;
            }
            inQueues.decrementAndGet();
            if (fiq.hasFreeSlot() && !fiq.queue.isEmpty()) {
                // multi-threaded queue: let another thread pick the next one
                schedule(fiq, fiq.getNextFetchTime());
            }
            return it;
        }
        return null;
    }

    public int numQueues() {
        return queues.size();
    }

    public int numQueuedItems() {
        return inQueues.get();
    }

    /** Default delay between two fetches from a queue, fetcher.server.delay in milliseconds. */
    public long defaultCrawlDelay() {
        return crawlDelay;
    }

    /**
     * Describes every fetch queue: its ID, size, fetches in progress and the URLs waiting. The
     * fetcher threads keep working meanwhile, so the figures may not add up exactly.
     */
    public String dump() {
        StringBuilder sb = new StringBuilder();
        sb.append("\nNum queues : ").append(queues.size());
        for (Entry<String, FetchItemQueue> entry : queues.entrySet()) {
            sb.append("\nQueue ID : ").append(entry.getKey());
            FetchItemQueue fiq = entry.getValue();
            sb.append("\t size : ").append(fiq.getQueueSize());
            sb.append("\t in progress : ").append(fiq.getInProgressSize());
            for (FetchItem fetchItem : fiq.queue) {
                sb.append("\n\t").append(fetchItem.url());
            }
        }
        return sb.toString();
    }

    /**
     * This class handles FetchItems which come from the same host ID (be it a proto/hostname or
     * proto/IP pair). It also keeps track of requests in progress and elapsed time between
     * requests.
     */
    public static class FetchItemQueue {
        final Queue<FetchItem> queue = new ConcurrentLinkedQueue<>();

        final String id;

        /** Number of items in {@link #queue}; bounded by maxQueueSize. */
        private final AtomicInteger size = new AtomicInteger();

        private final AtomicInteger inProgress = new AtomicInteger();
        private final AtomicLong nextFetchTime = new AtomicLong();

        /** Whether a ticket for this queue is currently present in the ready queue. */
        private final AtomicBoolean scheduled = new AtomicBoolean(false);

        /** Set when the queue has been removed from the map because it was empty. */
        private boolean removed = false;

        private final int maxQueueSize;
        private final int maxThreads;

        /**
         * Per-queue delays. Raised by {@link FetchItemQueues#getFetchItemQueue} with an atomic max,
         * so two URLs for the same host arriving together with different delays always settle on
         * the larger one; set outright by the fetcher thread when robots.txt says otherwise.
         */
        final AtomicLong minCrawlDelay;

        final AtomicLong crawlDelay;

        /** Consecutive fetches of this queue rejected because every helper thread was busy. */
        private final AtomicInteger saturations = new AtomicInteger();

        FetchItemQueue(
                String id, int maxThreads, long crawlDelay, long minCrawlDelay, int maxQueueSize) {
            this.id = id;
            this.maxThreads = maxThreads;
            this.crawlDelay = new AtomicLong(crawlDelay);
            this.minCrawlDelay = new AtomicLong(minCrawlDelay);
            this.maxQueueSize = maxQueueSize;
            // ready to start
            setNextFetchTime(System.currentTimeMillis(), true);
        }

        int getQueueSize() {
            // never negative for the metrics, even while a poll of an empty queue is in flight
            return Math.max(0, size.get());
        }

        int getInProgressSize() {
            return inProgress.get();
        }

        /**
         * The crawl delay of this queue, in milliseconds. A queue allowing more than one thread
         * spaces its fetches by the min crawl delay instead.
         */
        public long getCrawlDelay() {
            return crawlDelay.get();
        }

        /**
         * Sets the crawl delay of this queue, in milliseconds. It overwrites the value: unlike the
         * maximum taken when URLs are added, it can lower the delay.
         */
        public void setCrawlDelay(long delay) {
            crawlDelay.set(delay);
        }

        long getNextFetchTime() {
            return nextFetchTime.get();
        }

        /**
         * Must be called with the monitor of this queue held, so offers never overlap. The size is
         * incremented before the bound is checked and decremented again on overflow, so {@link
         * #getQueueSize()} can transiently over-report by one while an offer is being rejected;
         * since offers are serialised, that cannot make another offer fail.
         */
        boolean offer(FetchItem it) {
            if (removed) {
                return false;
            }
            if (size.incrementAndGet() > maxQueueSize) {
                size.decrementAndGet();
                return false;
            }
            queue.add(it);
            return true;
        }

        /**
         * Takes the next item, or returns null if the queue is empty or all its slots are taken.
         *
         * <p>The slot is reserved <em>before</em> the item is dequeued and released again if there
         * was none: while an item is being handed out, {@link #getInProgressSize()} is never zero,
         * so a fetch finishing concurrently on this queue cannot reap it from under the item.
         * Reserving with a CAS also makes the bound exact when several threads race for the last
         * slot of a multi-threaded queue.
         *
         * <p>The size is decremented <em>before</em> the dequeue for the mirror reason: {@link
         * #offer} bounds on it without holding anything a poll holds, and a counter lagging behind
         * the dequeue would refuse an add for a queue that has room. Under-reporting by one while a
         * poll is in flight can neither refuse an add nor push the real size past the bound, since
         * the poll removes an item right after.
         */
        FetchItem poll() {
            if (!tryAcquireSlot()) {
                return null;
            }
            size.decrementAndGet();
            FetchItem it = queue.poll();
            if (it == null) {
                size.incrementAndGet();
                inProgress.decrementAndGet();
                return null;
            }
            afterDequeue();
            return it;
        }

        /** Test hook, called right after an item has been taken from the queue. No-op. */
        void afterDequeue() {}

        private boolean tryAcquireSlot() {
            int current;
            do {
                current = inProgress.get();
                if (current >= maxThreads) {
                    return false;
                }
            } while (!inProgress.compareAndSet(current, current + 1));
            return true;
        }

        boolean hasFreeSlot() {
            return inProgress.get() < maxThreads;
        }

        boolean isReady(long now) {
            return nextFetchTime.get() <= now;
        }

        void finish(boolean asap) {
            inProgress.decrementAndGet();
            saturations.set(0);
            setNextFetchTime(System.currentTimeMillis(), asap);
        }

        /**
         * Like {@link #finish} for a fetch which never ran because every helper thread was busy.
         * The helpers are shared by all queues and are not freed by trying again, so instead of
         * being ready at once the queue is backed off exponentially: its own delay, doubled at each
         * consecutive rejection, up to {@code maxBackoff} or its own delay if longer. Any other
         * outcome resets the backoff.
         *
         * <p>The delay is spread with equal jitter, between half the computed value and the value
         * itself: a stuck protocol rejects the fetches of many queues within the same instant, and
         * without jitter they would all become ready together and hit the pool as a herd again.
         *
         * @return the delay applied, in milliseconds
         */
        long finishSaturated(long maxBackoff) {
            inProgress.decrementAndGet();
            // 2^30 x 1 s is already far beyond any sensible cap: bound the shift, not the counter
            final int rejections = Math.min(saturations.incrementAndGet(), 30);
            final long base =
                    Math.max(1000L, maxThreads > 1 ? minCrawlDelay.get() : crawlDelay.get());
            // the cap never shortens the queue's own delay
            final long computed = Math.min(Math.max(maxBackoff, base), base << (rejections - 1));
            final long half = computed / 2;
            final long delay = half + ThreadLocalRandom.current().nextLong(computed - half + 1);
            nextFetchTime.set(System.currentTimeMillis() + delay);
            return delay;
        }

        private void setNextFetchTime(long endTime, boolean asap) {
            if (!asap) {
                nextFetchTime.set(
                        endTime + (maxThreads > 1 ? minCrawlDelay.get() : crawlDelay.get()));
            } else {
                nextFetchTime.set(endTime);
            }
        }
    }

    /**
     * A ticket in the ready queue: a queue which may have an item to fetch at {@code time}. Kept
     * separate from the queue itself so that the ordering key is immutable while in the heap.
     */
    private record QueueTicket(FetchItemQueue fiq, long time) implements Delayed {

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(time - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(time, ((QueueTicket) o).time);
        }
    }
}
