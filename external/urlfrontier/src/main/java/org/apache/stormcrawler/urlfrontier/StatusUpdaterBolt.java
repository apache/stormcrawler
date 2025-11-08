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

import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_ADDRESS_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_CACHE_EXPIREAFTER_SEC_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_CRAWL_ID_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_DEFAULT_HOST;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_DEFAULT_PORT;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_HOST_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_MAX_MESSAGES_IN_FLIGHT_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_PORT_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_THROTTLING_TIME_MS_KEY;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_UPDATER_MAX_MESSAGES_KEY;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.base.Joiner;
import crawlercommons.urlfrontier.CrawlID;
import crawlercommons.urlfrontier.URLFrontierGrpc;
import crawlercommons.urlfrontier.URLFrontierGrpc.URLFrontierStub;
import crawlercommons.urlfrontier.Urlfrontier.AckMessage;
import crawlercommons.urlfrontier.Urlfrontier.DiscoveredURLItem;
import crawlercommons.urlfrontier.Urlfrontier.KnownURLItem;
import crawlercommons.urlfrontier.Urlfrontier.StringList;
import crawlercommons.urlfrontier.Urlfrontier.StringList.Builder;
import crawlercommons.urlfrontier.Urlfrontier.URLInfo;
import crawlercommons.urlfrontier.Urlfrontier.URLItem;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.URLPartitioner;
import org.apache.tika.utils.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt
        implements RemovalListener<String, List<Tuple>>,
                StreamObserver<crawlercommons.urlfrontier.Urlfrontier.AckMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(StatusUpdaterBolt.class);

    private ManagedChannel channel;
    private URLPartitioner partitioner;
    private StreamObserver<URLItem> requestObserver;

    private Cache<String, List<Tuple>> waitAck;

    // We have to prevent starving caused by the cache-timeout. Therefore, sophisticated lock with
    // fairness.
    private final ReentrantLock waitAckLock = new ReentrantLock(true);

    private int maxMessagesInFlight = 100000;
    private long throttleTimeMS;

    // Faster ways of locking until n messages are processed
    private Semaphore inFlightSemaphore;

    private MultiCountMetric eventCounter;

    /** Globally set crawlID * */
    private String globalCrawlID;

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        var expireAfterNMillisec =
                ConfUtils.getLong(stormConf, URLFRONTIER_CACHE_EXPIREAFTER_SEC_KEY, 60);

        waitAck =
                Caffeine.newBuilder()
                        .expireAfterWrite(expireAfterNMillisec, TimeUnit.SECONDS)
                        .removalListener(this)
                        .build();

        maxMessagesInFlight =
                ConfUtils.getInt(
                        stormConf, URLFRONTIER_MAX_MESSAGES_IN_FLIGHT_KEY, maxMessagesInFlight);

        throttleTimeMS = ConfUtils.getLong(stormConf, URLFRONTIER_THROTTLING_TIME_MS_KEY, 10);

        eventCounter =
                context.registerMetric(this.getClass().getSimpleName(), new MultiCountMetric(), 30);

        maxMessagesInFlight =
                ConfUtils.getInt(
                        stormConf, URLFRONTIER_UPDATER_MAX_MESSAGES_KEY, maxMessagesInFlight);

        LOG.info("Allowing up to {} message(s) in flight", maxMessagesInFlight);

        // Fairness not necessary, we are not in a hurry, as long as we may be processed at some
        // point.
        inFlightSemaphore = new Semaphore(maxMessagesInFlight, false);

        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);

        globalCrawlID = ConfUtils.getString(stormConf, URLFRONTIER_CRAWL_ID_KEY, CrawlID.DEFAULT);

        // host and port of URL Frontier(s)
        List<String> addresses = ConfUtils.loadListFromConf(URLFRONTIER_ADDRESS_KEY, stormConf);

        // Selected address
        String address;
        switch (addresses.size()) {
            case 0:
                LOG.debug("{} has no addresses.", URLFRONTIER_ADDRESS_KEY);
                address = null;
                break;
            case 1:
                LOG.debug(
                        "{} with a size of {} is used.", URLFRONTIER_ADDRESS_KEY, addresses.size());
                address = addresses.get(0);
                break;
            default:
                LOG.debug(
                        "{} with a size of {} is used.", URLFRONTIER_ADDRESS_KEY, addresses.size());
                int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
                // check that the number of tasks is a multiple of the frontier nodes
                if (totalTasks < addresses.size()) {
                    String message =
                            "Needs at least one task per frontier node. "
                                    + totalTasks
                                    + " vs "
                                    + addresses.size();
                    LOG.error(message);
                    throw new RuntimeException(message);
                }

                if (totalTasks % addresses.size() != 0) {
                    String message =
                            "Number of tasks not a multiple of the number of frontier nodes. "
                                    + totalTasks
                                    + " vs "
                                    + addresses.size();
                    LOG.error(message);
                    throw new RuntimeException(message);
                }

                int nodeIndex = context.getThisTaskIndex();
                int assignment = nodeIndex % addresses.size();
                Collections.sort(addresses);
                address = addresses.get(assignment);
        }

        if (address == null) {
            String host =
                    ConfUtils.getString(stormConf, URLFRONTIER_HOST_KEY, URLFRONTIER_DEFAULT_HOST);
            int port = ConfUtils.getInt(stormConf, URLFRONTIER_PORT_KEY, URLFRONTIER_DEFAULT_PORT);
            address = host + ":" + port;
        }

        channel = ManagedChannelUtil.createChannel(address);
        channel.notifyWhenStateChanged(
                ConnectivityState.SHUTDOWN, () -> onChannelStateChange(ConnectivityState.SHUTDOWN));
        URLFrontierStub frontier = URLFrontierGrpc.newStub(channel).withWaitForReady();
        requestObserver = frontier.putURLs(this);
    }

    private void onChannelStateChange(ConnectivityState state) {
        ConnectivityState newState = channel.getState(true);
        LOG.debug("Channel state changed from {} to {}", state, newState);
        if (state == ConnectivityState.TRANSIENT_FAILURE) {
            URLFrontierStub frontier = URLFrontierGrpc.newStub(channel).withWaitForReady();
            requestObserver = frontier.putURLs(this);
        }
        channel.notifyWhenStateChanged(newState, () -> onChannelStateChange(newState));
    }

    @Override
    public void onNext(final crawlercommons.urlfrontier.Urlfrontier.AckMessage confirmation) {
        // use the URL as ID
        final String url = confirmation.getID();

        List<Tuple> values;

        waitAckLock.lock();
        try {
            values = waitAck.getIfPresent(url);
            if (values != null) {
                // Invalidate before releasing permits to protect from new entries for this URL
                // until permits are handed out. Invalidate removes the key url from waitAck,
                // therefore it is safe to use values without lock at this point.
                waitAck.invalidate(url);
            }
        } finally {
            waitAckLock.unlock();
        }

        if (values == null) {
            // This should not happen, but breach of URLFrontier-protocol can.
            if (StringUtils.isBlank(url)) {
                LOG.warn(
                        "Could not find unacked tuple for a blank id (url). (id=`{}`, ack={})",
                        url,
                        confirmation);
                if (LOG.isTraceEnabled()) {
                    var fields = confirmation.getAllFields();
                    if (fields.isEmpty()) {
                        LOG.trace(
                                "There are no fields in the AckMessage for the unacked tuple for the blank id.");
                    } else {
                        LOG.trace(
                                "Fields in AckMessage for the unacked tuple for a blank id: {}",
                                Joiner.on(",").withKeyValueSeparator("=").join(fields));
                    }
                }
            } else {
                LOG.debug("Could not find unacked tuple for id `{}`.", url);
            }
            return;
        }

        // We release all permits in one go before handling the ACK-status.
        inFlightSemaphore.release(values.size());

        final boolean hasFailed = confirmation.getStatus().equals(AckMessage.Status.FAIL);
        if (!hasFailed) {
            LOG.debug("Acked {} tuple(s) for ID {}", values.size(), url);
            for (Tuple t : values) {
                eventCounter.scope("acked").incrBy(1);
                super.ack(t, url);
            }
        } else {
            LOG.info("Failed {} tuple(s) for ID {}", values.size(), url);
            for (Tuple t : values) {
                eventCounter.scope("failed").incrBy(1);
                collector.fail(t);
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        LOG.error("Error received: {}", t.getMessage());
        LOG.debug("Error received", t);
    }

    @Override
    public void onCompleted() {
        // end of stream - nothing special to do?
    }

    @Override
    public void store(
            @NotNull String url,
            @NotNull Status status,
            @NotNull Metadata metadata,
            @NotNull Optional<Date> nextFetch,
            @NotNull Tuple t) {

        // First get processing permit. Otherwise, starvation possible.
        var hasPermit = false;
        var timeSpent = 0L;
        while (!hasPermit) {
            try {
                hasPermit = inFlightSemaphore.tryAcquire(throttleTimeMS, TimeUnit.MILLISECONDS);
                if (!hasPermit) {
                    LOG.trace(
                            "{} messages in flight, time spent throttling {}",
                            inFlightSemaphore.getQueueLength(),
                            timeSpent);
                    eventCounter.scope("timeSpentThrottling").incrBy(throttleTimeMS);
                    timeSpent += throttleTimeMS;
                    if (timeSpent >= 30000L) {
                        LOG.warn(
                                "Waiting more than {} ms for processing. There are {} permits available for {} waiting threads.",
                                timeSpent,
                                inFlightSemaphore.availablePermits(),
                                inFlightSemaphore.getQueueLength());
                    }
                    // To prevent a deadlock, it is necessary to periodically clean up the waitAck
                    // cache. Otherwise, in case of a frontier-side or connection-wise error, all
                    // incoming URLs will after some time be all caught up in this loop without
                    // touching the cache, possibly leading to no eviction and thus leading to no
                    // release of inFlightSemaphore permits.
                    waitAckLock.lock();
                    try {
                        waitAck.cleanUp();
                    } finally {
                        waitAckLock.unlock();
                    }
                }
            } catch (InterruptedException e) {
                LOG.warn(
                        "InterruptedException - (approx.) {} messages in flight.",
                        inFlightSemaphore.getQueueLength());
                Thread.currentThread().interrupt();
            }
        }

        boolean urlIsNotBeingSentToTheFrontier;

        // only 1 thread at a time will access the store method
        // but onNext() might try to access waitAck at the same time
        waitAckLock.lock();
        try {
            // tuples received for the same URL
            // could be the same URL discovered from different pages
            // at the same time
            // or a page fetched linking to itself
            List<Tuple> tt = waitAck.get(url, k -> new LinkedList<>());

            // check that the same URL is not being sent to the frontier
            urlIsNotBeingSentToTheFrontier = status.equals(Status.DISCOVERED) && !tt.isEmpty();

            if (!urlIsNotBeingSentToTheFrontier) {
                // Permit will be released in onNext
                tt.add(t);
                // This slows us down, but no normal user would trace. So that is fine.
                LOG.trace(
                        "Added to waitAck {} with ID {} total {} - sent to {}",
                        url,
                        url,
                        tt.size(),
                        channel.authority());
            }
        } finally {
            waitAckLock.unlock();
        }

        if (urlIsNotBeingSentToTheFrontier) {
            // Release permit, because we will ACK fast if this url is already known and in the ack
            // process.
            inFlightSemaphore.release();
            // if this object is discovered - adding another version of it
            // won't make any difference
            LOG.debug("Already being sent to urlfrontier {} with status {}", url, status);
            // ack straight away!
            eventCounter.scope("acked").incrBy(1);
            super.ack(t, url);
            return;
        }

        String partitionKey = partitioner.getPartition(url, metadata);
        if (partitionKey == null) {
            partitionKey = "_DEFAULT_";
        }

        final Map<String, StringList> mdCopy = new HashMap<>(metadata.size());
        for (String k : metadata.keySet()) {
            String[] vals = metadata.getValues(k);
            if (vals != null) {
                Builder builder = StringList.newBuilder();
                for (String v : vals) builder.addValues(v);
                mdCopy.put(k, builder.build());
            }
        }

        URLInfo info =
                URLInfo.newBuilder()
                        .setKey(partitionKey)
                        .setUrl(url)
                        .setCrawlID(globalCrawlID)
                        .putAllMetadata(mdCopy)
                        .build();

        crawlercommons.urlfrontier.Urlfrontier.URLItem.Builder itemBuilder = URLItem.newBuilder();
        if (status.equals(Status.DISCOVERED)) {
            itemBuilder.setDiscovered(DiscoveredURLItem.newBuilder().setInfo(info).build());
        } else {
            // next fetch date
            long date = 0;
            if (nextFetch.isPresent()) {
                date = nextFetch.get().toInstant().getEpochSecond();
            }
            itemBuilder.setKnown(
                    KnownURLItem.newBuilder().setInfo(info).setRefetchableFromDate(date).build());
        }

        requestObserver.onNext(itemBuilder.setID(url).build());
    }

    @Override
    public void onRemoval(
            @Nullable String key, @Nullable List<Tuple> values, @NotNull RemovalCause cause) {

        // explicit removal (like Replace), we expect the removing code to release the permit if
        // necessary. (like cache.invalidate(url))
        if (!cause.wasEvicted()) {
            if (values != null) {
                LOG.trace(
                        "Evicted {} from waitAck with {} values. [{}]", key, values.size(), cause);
            } else {
                LOG.trace("Evicted {} from waitAck with no values. [{}]", key, cause);
            }
            return;
        }

        if (values != null) {
            // If we have values, we release their permits, because they are evicted by policy.
            inFlightSemaphore.release(values.size());
            var permits = inFlightSemaphore.availablePermits();
            LOG.warn("Evicted {} from waitAck with {} values. [{}]", key, values.size(), cause);

            if (permits < 0) {
                // This is a hint that we made a mistake.
                LOG.warn(
                        "Removing more elements than possible, the semaphore is negative {}.",
                        permits);
            }

            for (Tuple t : values) {
                eventCounter.scope("failed").incrBy(1);
                collector.fail(t);
            }
        } else {
            // This should never happen, but log it anyway.
            LOG.error("Evicted {} from waitAck with no values. [{}]", key, cause);
        }
    }

    @Override
    public void cleanup() {
        requestObserver.onCompleted();
        if (!channel.isShutdown()) {
            LOG.info("Shutting down connection to URLFrontier service.");
            channel.shutdown();
        } else {
            LOG.warn(
                    "Tried to shutdown connection to URLFrontier service that was already shutdown.");
        }
    }
}
