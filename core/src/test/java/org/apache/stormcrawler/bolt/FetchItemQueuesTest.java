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
}
