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

package org.apache.stormcrawler.bolt;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.Config;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.bolt.FetcherBolt.FetchItem;
import org.apache.stormcrawler.bolt.FetcherBolt.FetchItemQueue;
import org.apache.stormcrawler.bolt.FetcherBolt.FetchItemQueues;
import org.apache.stormcrawler.util.URLUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class FetchItemQueuesTest {

    private static FetchItemQueues queues(Object... kv) {
        Config conf = new Config();
        for (int i = 0; i < kv.length; i += 2) {
            conf.put((String) kv[i], kv[i + 1]);
        }
        return new FetchItemQueues(conf);
    }

    private static Tuple tuple(Metadata md) {
        Tuple t = mock(Tuple.class);
        when(t.contains("key")).thenReturn(false);
        when(t.getValueByField("metadata")).thenReturn(md);
        return t;
    }

    private static boolean add(FetchItemQueues q, String url) throws MalformedURLException {
        return add(q, url, new Metadata());
    }

    private static boolean add(FetchItemQueues q, String url, Metadata md)
            throws MalformedURLException {
        URL u = URLUtil.toURL(url);
        return q.addFetchItem(u, url, tuple(md));
    }

    private static FetchItem awaitItem(FetchItemQueues q, long maxMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + maxMillis;
        while (System.currentTimeMillis() < deadline) {
            FetchItem it = q.getFetchItem();
            if (it != null) {
                return it;
            }
            Thread.sleep(5);
        }
        return null;
    }

    @Test
    void itemIsReturnedOnceAndHostWaitsForCrawlDelay() throws Exception {
        FetchItemQueues q = queues("fetcher.server.delay", 0.3f);
        Assertions.assertTrue(add(q, "http://a.net/1"));
        Assertions.assertTrue(add(q, "http://a.net/2"));
        Assertions.assertEquals(2, q.inQueues.get());

        FetchItem first = q.getFetchItem();
        Assertions.assertNotNull(first);
        Assertions.assertEquals("http://a.net/1", first.url);
        Assertions.assertEquals(1, q.inQueues.get());
        // one thread per host: nothing else from a.net while the fetch is in progress
        Assertions.assertNull(q.getFetchItem());

        q.finishFetchItem(first, false);
        // still nothing: the crawl delay has not elapsed
        Assertions.assertNull(q.getFetchItem());
        FetchItem second = awaitItem(q, 2000);
        Assertions.assertNotNull(second);
        Assertions.assertEquals("http://a.net/2", second.url);
        Assertions.assertEquals(0, q.inQueues.get());
    }

    @Test
    void finishingAsapMakesHostImmediatelyAvailable() throws Exception {
        FetchItemQueues q = queues("fetcher.server.delay", 5.0f);
        add(q, "http://a.net/1");
        add(q, "http://a.net/2");
        FetchItem first = q.getFetchItem();
        q.finishFetchItem(first, true);
        FetchItem second = q.getFetchItem();
        Assertions.assertNotNull(second);
        Assertions.assertEquals("http://a.net/2", second.url);
    }

    @Test
    void differentHostsAreServedBackToBack() throws Exception {
        FetchItemQueues q = queues("fetcher.server.delay", 5.0f);
        add(q, "http://a.net/1");
        add(q, "http://b.net/1");
        add(q, "http://c.net/1");
        Set<String> got = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < 3; i++) {
            FetchItem it = q.getFetchItem();
            Assertions.assertNotNull(it);
            got.add(it.queueId);
        }
        Assertions.assertEquals(Set.of("a.net", "b.net", "c.net"), got);
        Assertions.assertNull(q.getFetchItem());
    }

    @Test
    void maxQueueSizeRejectsExtraItems() throws Exception {
        FetchItemQueues q = queues("fetcher.max.queue.size", 2);
        Assertions.assertTrue(add(q, "http://a.net/1"));
        Assertions.assertTrue(add(q, "http://a.net/2"));
        Assertions.assertFalse(add(q, "http://a.net/3"));
        Assertions.assertTrue(add(q, "http://b.net/1"));
        Assertions.assertEquals(3, q.inQueues.get());
    }

    @Test
    void multipleThreadsPerQueueAllowConcurrentFetchesFromSameHost() throws Exception {
        FetchItemQueues q = queues("fetcher.threads.per.queue", 2, "fetcher.server.delay", 5.0f);
        add(q, "http://a.net/1");
        add(q, "http://a.net/2");
        add(q, "http://a.net/3");
        FetchItem first = q.getFetchItem();
        FetchItem second = q.getFetchItem();
        Assertions.assertNotNull(first);
        Assertions.assertNotNull(second);
        // two in progress: the third has to wait
        Assertions.assertNull(q.getFetchItem());
        q.finishFetchItem(first, true);
        Assertions.assertNotNull(q.getFetchItem());
    }

    @Test
    void crawlDelayFromMetadataOverridesDefault() throws Exception {
        FetchItemQueues q = queues("fetcher.server.delay", 5.0f);
        Metadata md = new Metadata();
        md.setValue("crawl.delay", "0");
        add(q, "http://a.net/1", md);
        add(q, "http://a.net/2", md);
        FetchItem first = q.getFetchItem();
        q.finishFetchItem(first, false);
        Assertions.assertNotNull(awaitItem(q, 500));
    }

    @Test
    void emptyQueuesAreRemoved() throws Exception {
        FetchItemQueues q = queues("fetcher.server.delay", 0.0f);
        add(q, "http://a.net/1");
        Assertions.assertEquals(1, q.queues.size());
        FetchItem it = q.getFetchItem();
        q.finishFetchItem(it, false);
        // drained: the host must not stay in memory forever
        Assertions.assertNull(awaitItem(q, 200));
        Assertions.assertEquals(0, q.queues.size());
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void concurrentProducersAndConsumersLoseNothing() throws Exception {
        final int hosts = 200;
        final int perHost = 50;
        final int total = hosts * perHost;
        FetchItemQueues q = queues("fetcher.server.delay", 0.0f);
        AtomicInteger fetched = new AtomicInteger();
        Set<String> seen = ConcurrentHashMap.newKeySet();
        AtomicBoolean failed = new AtomicBoolean();
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < 4; p++) {
            final int producer = p;
            threads.add(
                    new Thread(
                            () -> {
                                try {
                                    for (int h = producer; h < hosts; h += 4) {
                                        for (int u = 0; u < perHost; u++) {
                                            if (!add(q, "http://h" + h + ".net/" + u)) {
                                                failed.set(true);
                                            }
                                        }
                                    }
                                } catch (Exception e) {
                                    failed.set(true);
                                }
                            }));
        }
        for (int c = 0; c < 16; c++) {
            threads.add(
                    new Thread(
                            () -> {
                                while (fetched.get() < total && !failed.get()) {
                                    FetchItem it = q.getFetchItem();
                                    if (it == null) {
                                        Thread.yield();
                                        continue;
                                    }
                                    if (!seen.add(it.url)) {
                                        failed.set(true);
                                    }
                                    fetched.incrementAndGet();
                                    q.finishFetchItem(it, false);
                                }
                            }));
        }
        threads.forEach(Thread::start);
        for (Thread t : threads) {
            t.join();
        }
        Assertions.assertFalse(failed.get(), "duplicate, lost or rejected item");
        Assertions.assertEquals(total, seen.size());
        Assertions.assertEquals(0, q.inQueues.get());
    }

    /**
     * A fetch finishing between the "no free slot" check and the clearing of the scheduled flag
     * must not leave the queue without a ticket: the URL waiting behind would never be fetched.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void fetchFinishingDuringFreeSlotCheckDoesNotLoseTheWakeup() throws Exception {
        FetchItemQueues q = queues("fetcher.server.delay", 0.0f);
        CountDownLatch inCheck = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean(false);
        // queue whose "no free slot" answer pauses until the test lets it continue
        FetchItemQueue hooked =
                new FetchItemQueue("a.net", 1, 0, 0, Integer.MAX_VALUE) {
                    @Override
                    boolean hasFreeSlot() {
                        boolean free = super.hasFreeSlot();
                        if (!free && armed.compareAndSet(true, false)) {
                            inCheck.countDown();
                            try {
                                proceed.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        return free;
                    }
                };
        q.queues.put("a.net", hooked);

        add(q, "http://a.net/1");
        FetchItem first = q.getFetchItem();
        Assertions.assertNotNull(first);
        // arrives while the first is in progress: issues a ticket
        add(q, "http://a.net/2");
        armed.set(true);

        FetchItem[] polled = new FetchItem[1];
        Thread poller = new Thread(() -> polled[0] = q.getFetchItem());
        poller.start();
        // the poller is now inside getFetchItem, having seen no free slot
        inCheck.await();
        // the first fetch finishes: its schedule() finds the flag still set
        q.finishFetchItem(first, true);
        proceed.countDown();
        poller.join();

        // whoever polls next must get the second URL
        FetchItem second = polled[0] != null ? polled[0] : awaitItem(q, 2000);
        Assertions.assertNotNull(second, "second URL lost: no ticket left for the queue");
        Assertions.assertEquals("http://a.net/2", second.url);
    }

    /**
     * A fetch finishing on the same queue while another thread is between taking the last item and
     * accounting for it must not reap the queue: the item being dispatched would otherwise finish
     * on an unknown or replacement queue and the in-progress counter would drift.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void fetchFinishingDuringPollDoesNotReapTheQueue() throws Exception {
        FetchItemQueues q = queues("fetcher.server.delay", 0.0f);
        CountDownLatch inPoll = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean(false);
        // two threads per queue; the dequeue pauses until the test lets it continue
        FetchItemQueue hooked =
                new FetchItemQueue("a.net", 2, 0, 0, Integer.MAX_VALUE) {
                    @Override
                    void afterDequeue() {
                        if (armed.compareAndSet(true, false)) {
                            inPoll.countDown();
                            try {
                                proceed.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                };
        q.queues.put("a.net", hooked);

        add(q, "http://a.net/1");
        add(q, "http://a.net/2");
        FetchItem first = q.getFetchItem();
        Assertions.assertNotNull(first);
        armed.set(true);

        FetchItem[] polled = new FetchItem[1];
        Thread poller = new Thread(() -> polled[0] = q.getFetchItem());
        poller.start();
        // the poller has taken the last item and is about to account for it
        inPoll.await();
        // the other fetch on the queue finishes and sees an empty queue
        q.finishFetchItem(first, true);
        Assertions.assertSame(
                hooked, q.queues.get("a.net"), "queue reaped while an item was being dispatched");
        proceed.countDown();
        poller.join();

        Assertions.assertNotNull(polled[0]);
        Assertions.assertEquals("http://a.net/2", polled[0].url);
        Assertions.assertEquals(1, hooked.getInProgressSize());
        // the dispatched item finishes on its own queue, which is then reaped
        q.finishFetchItem(polled[0], true);
        Assertions.assertEquals(0, hooked.getInProgressSize());
        Assertions.assertNull(q.queues.get("a.net"), "idle queue not reaped");
    }

    /** poll() reserves the slot itself: it must not hand out more items than maxThreads. */
    @Test
    void pollEnforcesMaxThreadsAtomically() throws MalformedURLException {
        FetchItemQueue fiq = new FetchItemQueue("a.net", 1, 0, 0, Integer.MAX_VALUE);
        Tuple t = tuple(new Metadata());
        FetchItem a = FetchItem.create(URLUtil.toURL("http://a.net/1"), "http://a.net/1", t, null);
        FetchItem b = FetchItem.create(URLUtil.toURL("http://a.net/2"), "http://a.net/2", t, null);
        fiq.queue.add(a);
        fiq.queue.add(b);
        Assertions.assertSame(a, fiq.poll());
        Assertions.assertNull(fiq.poll(), "second item handed out with the only slot taken");
        Assertions.assertEquals(1, fiq.getInProgressSize());
        Assertions.assertEquals(1, fiq.queue.size(), "item lost on a refused poll");
        fiq.finish(true);
        Assertions.assertSame(b, fiq.poll());
        Assertions.assertNull(fiq.poll());
        Assertions.assertEquals(1, fiq.getInProgressSize());
    }

    /**
     * Two URLs for the same host arriving together with different crawl delays: the queue must end
     * up with the larger one. Racy by nature: two long-lived threads are aligned by a spin barrier
     * for many rounds. On the previous read-check-write this failed within a few thousand rounds.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void concurrentDelayUpdatesKeepTheMaximum() throws Exception {
        final int rounds = 300_000;
        FetchItemQueues q = queues("fetcher.server.delay", 0.0f);
        FetchItemQueue fiq = q.getFetchItemQueue("a.net", new Metadata());
        Metadata small = new Metadata();
        small.setValue("crawl.delay", "1000");
        small.setValue("crawl.min.delay", "10");
        Metadata large = new Metadata();
        large.setValue("crawl.delay", "2000");
        large.setValue("crawl.min.delay", "20");
        AtomicInteger go = new AtomicInteger();
        AtomicInteger done = new AtomicInteger();
        AtomicInteger lostRounds = new AtomicInteger();
        Thread other =
                new Thread(
                        () -> {
                            for (int r = 1; r <= rounds; r++) {
                                while (go.get() != r) {
                                    Thread.onSpinWait();
                                }
                                q.getFetchItemQueue("a.net", small);
                                done.set(r);
                            }
                        });
        other.start();
        for (int r = 1; r <= rounds; r++) {
            fiq.crawlDelay.set(0);
            fiq.minCrawlDelay.set(0);
            go.set(r);
            q.getFetchItemQueue("a.net", large);
            while (done.get() != r) {
                Thread.onSpinWait();
            }
            if (fiq.crawlDelay.get() != 2000 || fiq.minCrawlDelay.get() != 20) {
                lostRounds.incrementAndGet();
            }
        }
        other.join();
        Assertions.assertEquals(0, lostRounds.get(), "rounds where the larger delay was lost");
    }

    /**
     * A queue at its size bound: while a poll is taking the last item, an add must not be refused
     * because the size counter has not caught up yet. A refused add fails the tuple, and Storm
     * replays a URL that had room.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void addDuringPollIsNotRefusedAtTheSizeBound() throws Exception {
        FetchItemQueues q = queues("fetcher.server.delay", 0.0f);
        CountDownLatch inPoll = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean(false);
        FetchItemQueue hooked =
                new FetchItemQueue("a.net", 1, 0, 0, 1) {
                    @Override
                    void afterDequeue() {
                        if (armed.compareAndSet(true, false)) {
                            inPoll.countDown();
                            try {
                                proceed.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                };
        q.queues.put("a.net", hooked);
        Assertions.assertTrue(add(q, "http://a.net/1"));
        Assertions.assertFalse(add(q, "http://a.net/2"), "bound not enforced");
        armed.set(true);

        FetchItem[] polled = new FetchItem[1];
        Thread poller = new Thread(() -> polled[0] = q.getFetchItem());
        poller.start();
        inPoll.await();
        // the item is out of the queue: there is room for one more
        boolean added = add(q, "http://a.net/2");
        proceed.countDown();
        poller.join();

        Assertions.assertNotNull(polled[0]);
        Assertions.assertTrue(added, "add refused while the queue had room");
        Assertions.assertEquals(1, hooked.getQueueSize());
        Assertions.assertEquals(1, hooked.queue.size());
    }

    /**
     * A ticket whose queue had its delay extended after the ticket was issued must not make the
     * caller return empty-handed while another queue is due: the fetcher thread would sleep 100 ms
     * with work available.
     */
    @Test
    void staleTicketDoesNotHideAnotherReadyQueue() throws Exception {
        FetchItemQueues q = queues("fetcher.server.delay", 10.0f);
        add(q, "http://a.net/1");
        // strictly later next-fetch time for b.net, so a.net's ticket is always at the head
        Thread.sleep(5);
        add(q, "http://b.net/1");
        FetchItem a1 = q.getFetchItem();
        Assertions.assertNotNull(a1);
        Assertions.assertEquals("http://a.net/1", a1.url);
        // a second URL for a.net issues a ticket at the queue's current (past) next fetch time
        add(q, "http://a.net/2");
        // finishing with the 10 s delay pushes a.net's next fetch time into the future: the
        // ticket at the head of the ready queue is now stale
        q.finishFetchItem(a1, false);

        FetchItem next = q.getFetchItem();
        Assertions.assertNotNull(next, "returned nothing while b.net was ready");
        Assertions.assertEquals("http://b.net/1", next.url);
    }

    /** Asserts that the delay is the equal-jittered value: within [computed / 2, computed]. */
    private static void assertJittered(long computed, long delay) {
        Assertions.assertTrue(
                delay >= computed / 2 && delay <= computed,
                "delay " + delay + " not within [" + computed / 2 + ", " + computed + "]");
    }

    /**
     * A fetch rejected because every helper thread is busy backs its queue off exponentially from
     * the crawl delay up to fetcher.max.crawl.delay, with equal jitter, and a fetch which runs
     * resets the backoff.
     */
    @Test
    void saturatedFetchBacksTheQueueOffExponentially() throws Exception {
        FetchItemQueues q = queues("fetcher.server.delay", 2.0f, "fetcher.max.crawl.delay", 5);
        // enough items for the queue to stay alive while the slot is retaken below
        for (int i = 0; i < 8; i++) {
            add(q, "http://a.net/" + i);
        }
        FetchItem it = q.getFetchItem();
        Assertions.assertNotNull(it);
        FetchItemQueue fiq = q.queues.get(it.queueId);

        long[] expected = {2000, 4000, 5000, 5000};
        for (long computed : expected) {
            long before = System.currentTimeMillis();
            long delay = q.backOffFetchItem(it);
            assertJittered(computed, delay);
            long next = fiq.getNextFetchTime();
            Assertions.assertTrue(next >= before + delay, "next fetch time reflects the backoff");
            Assertions.assertTrue(next <= System.currentTimeMillis() + delay);
            Assertions.assertEquals(0, fiq.getInProgressSize(), "slot released");
            Assertions.assertNull(q.getFetchItem(), "queue not ready while backed off");
            // take the slot back for the next rejection
            fiq.poll();
            Assertions.assertEquals(1, fiq.getInProgressSize());
        }

        // a fetch which runs resets the backoff: the next rejection starts again from the delay
        q.finishFetchItem(it, true);
        fiq.poll();
        assertJittered(2000, q.backOffFetchItem(it));
    }

    /** The backoff never starts below one second, even for a queue with no delay at all. */
    @Test
    void saturatedFetchBackoffHasAOneSecondFloor() throws Exception {
        FetchItemQueues q = queues("fetcher.server.delay", 0.0f);
        add(q, "http://a.net/1");
        FetchItem it = q.getFetchItem();
        Assertions.assertNotNull(it);
        assertJittered(1000, q.backOffFetchItem(it));
    }

    /** The jitter actually spreads the delays: repeated rejections do not all get the same one. */
    @Test
    void saturatedFetchBackoffIsJittered() {
        Set<Long> delays = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < 50; i++) {
            FetchItemQueue fiq = new FetchItemQueue("h" + i, 1, 10000, 0, Integer.MAX_VALUE);
            fiq.poll();
            delays.add(fiq.finishSaturated(30000));
        }
        Assertions.assertTrue(delays.size() > 1, "every delay identical: " + delays);
    }
}
