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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.metrics.CrawlerMetrics;
import org.apache.stormcrawler.protocol.Protocol;
import org.apache.stormcrawler.util.ConfUtils;

/**
 * Enforces {@code fetcher.thread.timeout} on protocol calls (robots.txt lookup and fetch) for the
 * fetcher bolts.
 *
 * <p>When the timeout is off, or the protocol enforces it itself (see {@link
 * Protocol#supportsFetchTimeout()}, true for the default okhttp protocol), the call runs on the
 * calling thread. Otherwise it runs on a helper thread and is abandoned there when the deadline
 * passes: the helper stays busy until the protocol gives up on its own, so the pool is bounded
 * ({@code fetcher.thread.timeout.helpers}) and a call that finds every helper busy fails at once
 * with {@link SaturatedException}. Helper threads are created on demand and released after a minute
 * of inactivity: with the default protocol none is ever created.
 */
final class FetchTimeoutHelpers {

    /** Thrown when a call run on a helper thread did not complete within the timeout. */
    static final class TimeoutException extends Exception {
        TimeoutException(String url, long timeoutSecs) {
            super("Fetch timed out after " + timeoutSecs + "s fetching " + url);
        }
    }

    /** Thrown when no helper thread is available to run a call with a timeout. */
    static final class SaturatedException extends Exception {
        SaturatedException(String url) {
            super("No fetch helper available for " + url);
        }
    }

    private final long timeoutSecs;
    private final ThreadPoolExecutor helpers;

    /**
     * @param conf the bolt configuration
     * @param defaultMaxHelpers pool bound used unless {@code fetcher.thread.timeout.helpers} is set
     * @param threadNamePrefix prefix of the helper thread names
     */
    FetchTimeoutHelpers(Map<String, Object> conf, int defaultMaxHelpers, String threadNamePrefix) {
        this.timeoutSecs = ConfUtils.getLong(conf, Constants.FETCH_TIMEOUT_PARAM_KEY, -1);
        final int maxHelpers =
                ConfUtils.getInt(
                        conf, Constants.FETCH_TIMEOUT_HELPERS_PARAM_KEY, defaultMaxHelpers);
        final AtomicInteger helperNum = new AtomicInteger();
        this.helpers =
                new ThreadPoolExecutor(
                        0,
                        Math.max(1, maxHelpers),
                        60L,
                        TimeUnit.SECONDS,
                        new SynchronousQueue<>(),
                        r -> {
                            Thread t =
                                    new Thread(r, threadNamePrefix + helperNum.incrementAndGet());
                            t.setDaemon(true);
                            return t;
                        });
    }

    /** Registers the {@code fetchhelpers} gauge: number of helper threads busy with a call. */
    void registerMetrics(TopologyContext context, Map<String, Object> conf, int bucketSecs) {
        CrawlerMetrics.registerGauge(
                context, conf, "fetchhelpers", helpers::getActiveCount, bucketSecs);
    }

    /** Whether a timeout is configured at all. */
    boolean enabled() {
        return timeoutSecs > 0;
    }

    /** Timeout in seconds, -1 when disabled. */
    long timeoutSecs() {
        return timeoutSecs;
    }

    /** Pool bound. */
    int maxHelpers() {
        return helpers.getMaximumPoolSize();
    }

    /** Largest number of helper threads ever alive. */
    int largestPoolSize() {
        return helpers.getLargestPoolSize();
    }

    /**
     * Runs a protocol call under the timeout.
     *
     * @throws SaturatedException when every helper is busy
     * @throws TimeoutException when the deadline passed
     * @throws Exception the protocol's own exception
     */
    <T> T call(Callable<T> call, Protocol protocol, String url) throws Exception {
        if (timeoutSecs <= 0 || protocol.supportsFetchTimeout()) {
            return call.call();
        }
        final Future<T> future;
        try {
            future = helpers.submit(call);
        } catch (RejectedExecutionException e) {
            throw new SaturatedException(url);
        }
        try {
            return future.get(timeoutSecs, TimeUnit.SECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            future.cancel(true);
            throw new TimeoutException(url, timeoutSecs);
        } catch (CancellationException e) {
            throw new Exception("Fetch cancelled for " + url);
        } catch (ExecutionException e) {
            // unwrap the real cause so the bolts' classification sees it
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new Exception(cause);
        }
    }

    void shutdown() {
        helpers.shutdownNow();
    }
}
