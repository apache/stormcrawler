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
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_BATCH_SIZE_DEFAULT;
import static org.apache.stormcrawler.urlfrontier.Constants.URLFRONTIER_BATCH_SIZE_KEY;
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
import crawlercommons.urlfrontier.Urlfrontier.BatchAck;
import crawlercommons.urlfrontier.Urlfrontier.DiscoveredBatch;
import crawlercommons.urlfrontier.Urlfrontier.DiscoveredURLItem;
import crawlercommons.urlfrontier.Urlfrontier.KnownURLItem;
import crawlercommons.urlfrontier.Urlfrontier.StringList;
import crawlercommons.urlfrontier.Urlfrontier.StringList.Builder;
import crawlercommons.urlfrontier.Urlfrontier.URLInfo;
import crawlercommons.urlfrontier.Urlfrontier.URLItem;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.metrics.CrawlerMetrics;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.apache.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.URLPartitioner;
import org.apache.tika.utils.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persists the status of URLs in a URLFrontier service.
 *
 * <p>Known URLs (fetched, redirections, errors...) are sent one message at a time on the streaming
 * {@code PutURLs} endpoint. Discovered URLs, which are the bulk of what a crawl writes, are grouped
 * into batches and pushed on the batched {@code PutDiscovered} endpoint introduced in URLFrontier
 * 2.6, which amortises the per-message cost that limits the ingestion rate. A partially filled
 * batch is sent after a second at the latest, so that acks are not delayed when the crawl tails
 * off. If the frontier does not implement {@code PutDiscovered}, the bolt detects it and falls back
 * to sending discovered URLs individually on the streaming endpoint.
 *
 * <p>Flow control follows the client implementation shipped with URLFrontier
 * (crawlercommons.urlfrontier.client.PutURLs): the sends wait on a monitor woken by the acks and by
 * the transport's on-ready notifications, instead of polling.
 */
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt
        implements RemovalListener<String, List<Tuple>>,
                StreamObserver<crawlercommons.urlfrontier.Urlfrontier.AckMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(StatusUpdaterBolt.class);

    /** how long a partially filled batch is held back before it is sent anyway */
    private static final long BATCH_FLUSH_DELAY_MS = 1000;

    /** how often the flusher checks whether a batch is due */
    private static final long FLUSH_CHECK_INTERVAL_MS = 100;

    private ManagedChannel channel;
    private URLPartitioner partitioner;
    private volatile URLFrontierStub frontier;
    private volatile StreamObserver<URLItem> requestObserver;
    private volatile StreamObserver<DiscoveredBatch> batchRequestObserver;
    private volatile ClientCallStreamObserver<DiscoveredBatch> batchTransport;

    private Cache<String, List<Tuple>> waitAck;

    // We have to prevent starving caused by the cache-timeout. Therefore, sophisticated lock with
    // fairness.
    private final ReentrantLock waitAckLock = new ReentrantLock(true);

    private int maxMessagesInFlight = 100000;
    private long throttleTimeMS;

    // Faster ways of locking until n messages are processed
    private Semaphore inFlightSemaphore;

    private ScopedCounter eventCounter;

    /** Globally set crawlID * */
    private String globalCrawlID;

    /** max number of discovered URLs per batch message; 0 sends them individually */
    private volatile int batchSize = URLFRONTIER_BATCH_SIZE_DEFAULT;

    /** when the oldest item currently buffered was added, 0 when the buffer is empty */
    private long oldestBufferedAt;

    /** true as long as the frontier is expected to implement the PutDiscovered endpoint */
    private volatile boolean batching;

    /** discovered URLs waiting to be sent as one batch */
    private final ArrayDeque<URLItem> batchBuffer = new ArrayDeque<>();

    /** batches sent but not acked yet, keyed by the ID echoed back in the BatchAck */
    private final Map<String, List<URLItem>> pendingBatches = new HashMap<>();

    /** guards the batch buffer, the pending batches and the batch stream reference */
    private final Object batchLock = new Object();

    /**
     * guards the onNext calls on both gRPC streams: they come from the Storm executor thread and
     * from the gRPC callback threads
     */
    private final Object sendLock = new Object();

    private final AtomicInteger batchSequences = new AtomicInteger();

    private ScheduledExecutorService batchFlusher;

    /**
     * notified when permits are released and when a transport becomes ready, so that throttled
     * sends wake up as soon as they can proceed instead of polling
     */
    private final Object flow = new Object();

    /** set once the bolt is shutting down; the callbacks must not act on it anymore */
    private volatile boolean closed;

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
                CrawlerMetrics.registerCounter(
                        context, stormConf, this.getClass().getSimpleName(), 30);

        maxMessagesInFlight =
                ConfUtils.getInt(
                        stormConf, URLFRONTIER_UPDATER_MAX_MESSAGES_KEY, maxMessagesInFlight);

        batchSize = ConfUtils.getInt(stormConf, URLFRONTIER_BATCH_SIZE_KEY, batchSize);
        batching = batchSize > 0;

        LOG.info("Allowing up to {} message(s) in flight", maxMessagesInFlight);
        if (batching) {
            LOG.info("Sending up to {} discovered URL(s) per batch", batchSize);
        } else {
            LOG.info("Discovered URLs sent individually - batching disabled");
        }

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

        frontier = URLFrontierGrpc.newStub(channel).withWaitForReady();
        requestObserver = newPutURLsStream();

        if (batching) {
            batchRequestObserver = newPutDiscoveredStream();
            batchFlusher = Executors.newSingleThreadScheduledExecutor(
                    runnable -> {
                        Thread thread = new Thread(runnable, "URLFrontier-batch-flusher");
                        thread.setDaemon(true);
                        return thread;
                    });
            batchFlusher.scheduleWithFixedDelay(
                    this::flushBatchIfDue,
                    FLUSH_CHECK_INTERVAL_MS,
                    FLUSH_CHECK_INTERVAL_MS,
                    TimeUnit.MILLISECONDS);
        }
    }

    /** Opens a streaming PutURLs call whose acks are handled by this bolt. */
    private StreamObserver<URLItem> newPutURLsStream() {
        return frontier.putURLs(
                new ClientResponseObserver<URLItem, AckMessage>() {

                    @Override
                    public void beforeStart(ClientCallStreamObserver<URLItem> stream) {
                        stream.setOnReadyHandler(
                                () -> {
                                    synchronized (flow) {
                                        flow.notifyAll();
                                    }
                                });
                    }

                    @Override
                    public void onNext(AckMessage value) {
                        StatusUpdaterBolt.this.onNext(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        StatusUpdaterBolt.this.onError(t);
                    }

                    @Override
                    public void onCompleted() {
                        StatusUpdaterBolt.this.onCompleted();
                    }
                });
    }

    /** Opens a batched PutDiscovered call; the server acks a whole batch with one BatchAck. */
    private StreamObserver<DiscoveredBatch> newPutDiscoveredStream() {
        return frontier.putDiscovered(
                new ClientResponseObserver<DiscoveredBatch, BatchAck>() {

                    @Override
                    public void beforeStart(ClientCallStreamObserver<DiscoveredBatch> stream) {
                        batchTransport = stream;
                        stream.setOnReadyHandler(
                                () -> {
                                    synchronized (flow) {
                                        flow.notifyAll();
                                    }
                                });
                    }

                    @Override
                    public void onNext(BatchAck value) {
                        StatusUpdaterBolt.this.onNext(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        StatusUpdaterBolt.this.onBatchError(t);
                    }

                    @Override
                    public void onCompleted() {
                        // end of stream - nothing special to do?
                    }
                });
    }

    private void onChannelStateChange(ConnectivityState state) {
        ConnectivityState newState = channel.getState(true);
        LOG.debug("Channel state changed from {} to {}", state, newState);
        if (state == ConnectivityState.TRANSIENT_FAILURE) {
            requestObserver = newPutURLsStream();
            // the PutDiscovered stream is not recreated here: with waitForReady it survives
            // connection blips, and abandoning it would orphan the batches already sent on it
        }
        channel.notifyWhenStateChanged(newState, () -> onChannelStateChange(newState));
    }

    @Override
    public void onNext(final crawlercommons.urlfrontier.Urlfrontier.AckMessage confirmation) {
        // use the URL as ID
        final String url = confirmation.getID();

        final List<Tuple> values = detachWaitAck(url);

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

        completeTuples(url, values, confirmation.getStatus());
    }

    /**
     * Acknowledges a whole batch: one status per URL, in the order the batch was sent.
     *
     * @param confirmation the BatchAck received from the PutDiscovered endpoint
     */
    private void onNext(final BatchAck confirmation) {
        if (closed) {
            return;
        }

        final List<URLItem> items;
        synchronized (batchLock) {
            items = pendingBatches.remove(confirmation.getID());
        }

        if (items == null) {
            LOG.debug("Could not find batch with ID `{}`.", confirmation.getID());
            return;
        }

        final List<AckMessage.Status> statuses = confirmation.getStatusesList();
        if (statuses.size() != items.size()) {
            LOG.warn(
                    "BatchAck {} carries {} status(es) for {} URL(s).",
                    confirmation.getID(),
                    statuses.size(),
                    items.size());
        }

        // URLs without a status, e.g. on a protocol breach, are left to the waitAck eviction
        int numStatuses = Math.min(statuses.size(), items.size());
        for (int i = 0; i < numStatuses; i++) {
            final String url = items.get(i).getID();
            final List<Tuple> values = detachWaitAck(url);
            if (values == null) {
                LOG.debug("Could not find unacked tuple for id `{}`.", url);
                continue;
            }
            completeTuples(url, values, statuses.get(i));
        }
    }

    private void onBatchError(final Throwable t) {
        if (closed) {
            return;
        }

        // a frontier older than 2.6 does not know the PutDiscovered endpoint; instead of
        // dropping the discovered URLs, send them individually on the streaming endpoint
        // and keep doing so for the rest of the bolt's life
        if (t instanceof StatusRuntimeException
                && ((StatusRuntimeException) t).getStatus().getCode()
                        == io.grpc.Status.Code.UNIMPLEMENTED) {
            LOG.warn(
                    "The frontier does not implement PutDiscovered (URLFrontier < 2.6) - sending discovered URLs individually on the streaming endpoint.");
            disableBatchingAndResend();
            return;
        }

        LOG.error("Error received on the batch stream: {}", t.getMessage());
        LOG.debug("Error received on the batch stream", t);

        // the stream is dead: forget the batches it carried, their tuples are failed by the
        // waitAck cache eviction and replayed by Storm. A new stream is opened on the next flush.
        synchronized (batchLock) {
            pendingBatches.clear();
            batchRequestObserver = null;
            batchTransport = null;
        }
        synchronized (flow) {
            flow.notifyAll();
        }
    }

    /** Stops batching and pushes everything buffered or in flight through the streaming endpoint. */
    private void disableBatchingAndResend() {
        final List<URLItem> toResend = new ArrayList<>();
        synchronized (batchLock) {
            batching = false;
            for (List<URLItem> items : pendingBatches.values()) {
                toResend.addAll(items);
            }
            pendingBatches.clear();
            toResend.addAll(batchBuffer);
            batchBuffer.clear();
            oldestBufferedAt = 0;
            batchRequestObserver = null;
        }
        if (batchFlusher != null) {
            batchFlusher.shutdownNow();
        }
        if (toResend.isEmpty()) {
            return;
        }
        LOG.info("Re-sending {} discovered URL(s) individually.", toResend.size());
        for (URLItem item : toResend) {
            sendOnStreamingEndpoint(item);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (closed) {
            return;
        }
        LOG.error("Error received: {}", t.getMessage());
        LOG.debug("Error received", t);
        synchronized (flow) {
            flow.notifyAll();
        }
    }

    @Override
    public void onCompleted() {
        // end of stream - nothing special to do?
    }

    /**
     * Sends a URL item on the streaming endpoint, failing its tuples locally if the stream got
     * terminated while we were sending.
     */
    private void sendOnStreamingEndpoint(final URLItem item) {
        try {
            synchronized (sendLock) {
                requestObserver.onNext(item);
            }
        } catch (IllegalStateException e) {
            // the stream got terminated while we were sending
            LOG.debug("Failed to send {} on the streaming endpoint.", item.getID(), e);
            failTupleLocally(item.getID());
        }
    }

    /** Detaches the tuples waiting for the given URL from the waitAck cache. */
    @Nullable
    private List<Tuple> detachWaitAck(final String url) {
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
        return values;
    }

    /** Releases the permits and acks or fails the tuples of a completed URL. */
    private void completeTuples(
            final String url, @NotNull final List<Tuple> values, final AckMessage.Status status) {
        // We release all permits in one go before handling the ACK-status.
        releasePermits(values.size());

        final boolean hasFailed = status.equals(AckMessage.Status.FAIL);
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

    /**
     * Fails the tuples waiting for the given URL without waiting for an ack, e.g. because the
     * stream they were sent on got terminated. Storm replays them.
     */
    private void failTupleLocally(final String url) {
        final List<Tuple> values = detachWaitAck(url);
        if (values == null) {
            return;
        }
        releasePermits(values.size());
        for (Tuple t : values) {
            eventCounter.scope("failed").incrBy(1);
            collector.fail(t);
        }
    }

    /** Releases permits and wakes up any send waiting for room. */
    private void releasePermits(int numPermits) {
        inFlightSemaphore.release(numPermits);
        synchronized (flow) {
            flow.notifyAll();
        }
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
        boolean throttled = false;
        while (!hasPermit) {
            hasPermit = inFlightSemaphore.tryAcquire();
            if (!hasPermit) {
                throttled = true;
                LOG.trace(
                        "{} messages in flight, time spent throttling {}",
                        inFlightSemaphore.getQueueLength(),
                        timeSpent);
                // wait for room on the monitor: woken as soon as an ack releases permits or the
                // transport becomes ready again. The timeout is a backstop, not a poll interval.
                synchronized (flow) {
                    try {
                        flow.wait(throttleTimeMS);
                    } catch (InterruptedException e) {
                        LOG.warn(
                                "InterruptedException - (approx.) {} messages in flight.",
                                inFlightSemaphore.getQueueLength());
                        Thread.currentThread().interrupt();
                    }
                }
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
        }

        if (throttled) {
            // acks were slow: push out whatever is buffered so that permits free up again
            flushBatch();
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
            releasePermits(1);
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

        // send a tuple on the queue stream in case a bolt
        // wants to handle it
        super.collector.emit(
                org.apache.stormcrawler.Constants.QUEUE_STREAM_NAME,
                new Values(partitionKey, metadata));

        final Map<String, StringList> mdCopy = new HashMap<>(metadata.size());
        for (String k : metadata.keySet()) {
            String[] vals = metadata.getValues(k);
            if (vals != null) {
                Builder builder = StringList.newBuilder();
                for (String v : vals) {
                    builder.addValues(v);
                }
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

        final URLItem item = itemBuilder.setID(url).build();

        // discovered URLs travel in batches on the PutDiscovered endpoint, known URLs keep
        // using the streaming endpoint
        if (status.equals(Status.DISCOVERED)) {
            boolean shouldBatch;
            boolean flushNow = false;
            synchronized (batchLock) {
                // re-read inside the lock: batching can be disabled concurrently by the
                // fallback for frontiers without the PutDiscovered endpoint
                shouldBatch = batching;
                if (shouldBatch) {
                    if (batchBuffer.isEmpty()) {
                        oldestBufferedAt = System.currentTimeMillis();
                    }
                    batchBuffer.add(item);
                    flushNow = batchBuffer.size() >= batchSize;
                }
            }
            if (!shouldBatch) {
                sendOnStreamingEndpoint(item);
                return;
            }
            if (flushNow) {
                flushBatch();
            }
            return;
        }

        if (batching) {
            // the outlinks buffered so far belong to the page whose status is now updated:
            // a natural boundary for the batch
            flushBatch();
        }

        sendOnStreamingEndpoint(item);
    }

    /** Sends the buffered discovered URLs as one batch, if any. */
    private void flushBatch() {
        flushBatch(false);
    }

    /**
     * Sends the buffered discovered URLs as one batch, if any.
     *
     * @param awaitTransport whether to wait briefly for the transport to become ready before
     *     sending; only the flusher thread may do so, the Storm executor thread never stalls
     */
    private void flushBatch(boolean awaitTransport) {
        final List<URLItem> items;
        final StreamObserver<DiscoveredBatch> stream;
        synchronized (batchLock) {
            if (!batching || batchBuffer.isEmpty()) {
                return;
            }
            if (batchRequestObserver == null) {
                // the previous stream died: open a new one
                batchRequestObserver = newPutDiscoveredStream();
            }
            stream = batchRequestObserver;
            items = new ArrayList<>(batchBuffer);
            batchBuffer.clear();
            oldestBufferedAt = 0;
        }

        final String batchID = "batch-" + batchSequences.incrementAndGet();
        final DiscoveredBatch.Builder batchBuilder = DiscoveredBatch.newBuilder().setID(batchID);
        for (URLItem buffered : items) {
            batchBuilder.addItems(buffered.getDiscovered().getInfo());
        }
        final DiscoveredBatch batch = batchBuilder.build();

        // registered before the send so that a fast ack can never miss it
        synchronized (batchLock) {
            pendingBatches.put(batchID, items);
        }

        try {
            if (awaitTransport) {
                // follow the transport's lead: wait briefly for it to take the batch without
                // buffering it, woken by the on-ready handler. The timeout is a backstop, not a
                // poll interval.
                final ClientCallStreamObserver<DiscoveredBatch> transport = batchTransport;
                if (transport != null && !transport.isReady()) {
                    synchronized (flow) {
                        flow.wait(BATCH_FLUSH_DELAY_MS);
                    }
                }
            }
            synchronized (sendLock) {
                stream.onNext(batch);
            }
            eventCounter.scope("batched").incrBy(items.size());
            eventCounter.scope("batches").incrBy(1);
            LOG.debug("Sent batch {} with {} discovered URL(s).", batchID, items.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            // the stream got terminated while we were sending
            LOG.debug("Failed to send batch {}.", batchID, e);
            synchronized (batchLock) {
                pendingBatches.remove(batchID);
                if (batchRequestObserver == stream) {
                    batchRequestObserver = null;
                    batchTransport = null;
                }
            }
            for (URLItem failed : items) {
                failTupleLocally(failed.getID());
            }
        }
    }

    /** Flushes the buffer when it reached the batch size or its oldest item is old enough. */
    private void flushBatchIfDue() {
        if (closed) {
            return;
        }
        try {
            boolean due;
            synchronized (batchLock) {
                due =
                        batching
                                && !batchBuffer.isEmpty()
                                && (batchBuffer.size() >= batchSize
                                        || System.currentTimeMillis() - oldestBufferedAt
                                                >= BATCH_FLUSH_DELAY_MS);
            }
            if (due) {
                flushBatch(true);
            }
        } catch (RuntimeException e) {
            // must not kill the scheduled flusher
            LOG.error("Error while flushing the batch of discovered URLs.", e);
        }
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
            releasePermits(values.size());
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

    /** number of batch messages handed to the transport so far; also used by the tests */
    int batchesSent() {
        return batchSequences.get();
    }

    /** whether the discovered URLs are currently grouped into batches; also used by the tests */
    boolean isBatching() {
        return batching;
    }

    @Override
    public void cleanup() {
        closed = true;
        if (batchFlusher != null) {
            batchFlusher.shutdownNow();
        }
        // best effort: hand over whatever is still buffered
        try {
            flushBatch();
        } catch (RuntimeException e) {
            LOG.debug("Could not flush the remaining discovered URLs.", e);
        }
        try {
            requestObserver.onCompleted();
        } catch (RuntimeException e) {
            // the stream got terminated in the meantime
        }
        final StreamObserver<DiscoveredBatch> batchStream = batchRequestObserver;
        if (batchStream != null) {
            try {
                batchStream.onCompleted();
            } catch (RuntimeException e) {
                // the stream got terminated in the meantime
            }
        }
        if (!channel.isShutdown()) {
            LOG.info("Shutting down connection to URLFrontier service.");
            channel.shutdown();
        } else {
            LOG.warn(
                    "Tried to shutdown connection to URLFrontier service that was already shutdown.");
        }
    }
}
