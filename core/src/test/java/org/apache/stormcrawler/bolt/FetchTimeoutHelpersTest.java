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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.protocol.DummyProtocol;
import org.apache.stormcrawler.protocol.Protocol;
import org.apache.stormcrawler.protocol.StuckProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 20, unit = TimeUnit.SECONDS)
class FetchTimeoutHelpersTest {

    /** A protocol which claims to enforce the timeout itself. */
    private static final Protocol SELF_TIMING =
            new DummyProtocol() {
                @Override
                public boolean supportsFetchTimeout() {
                    return true;
                }
            };

    private static final Protocol PLAIN = new DummyProtocol();

    private FetchTimeoutHelpers helpers;

    private static Map<String, Object> conf(long timeoutSecs, Integer maxHelpers) {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Constants.FETCH_TIMEOUT_PARAM_KEY, timeoutSecs);
        if (maxHelpers != null) {
            conf.put(Constants.FETCH_TIMEOUT_HELPERS_PARAM_KEY, maxHelpers);
        }
        return conf;
    }

    private FetchTimeoutHelpers helpers(long timeoutSecs, Integer maxHelpers) {
        helpers = new FetchTimeoutHelpers(conf(timeoutSecs, maxHelpers), 4, "test-helper-");
        return helpers;
    }

    @AfterEach
    void shutdown() {
        if (helpers != null) {
            helpers.shutdown();
        }
    }

    @Test
    void disabledRunsOnTheCallingThread() throws Exception {
        FetchTimeoutHelpers h = helpers(-1, null);
        Assertions.assertFalse(h.enabled());
        Thread caller = Thread.currentThread();
        Thread ran = h.call(Thread::currentThread, PLAIN, "http://a.net/");
        Assertions.assertSame(caller, ran);
        Assertions.assertEquals(0, h.largestPoolSize());
    }

    @Test
    void protocolEnforcingTheTimeoutRunsOnTheCallingThread() throws Exception {
        FetchTimeoutHelpers h = helpers(1, null);
        Assertions.assertTrue(h.enabled());
        Assertions.assertEquals(1, h.timeoutSecs());
        Thread ran = h.call(Thread::currentThread, SELF_TIMING, "http://a.net/");
        Assertions.assertSame(Thread.currentThread(), ran);
        Assertions.assertEquals(0, h.largestPoolSize());
    }

    @Test
    void otherProtocolsRunOnAHelperAndReturnTheResult() throws Exception {
        FetchTimeoutHelpers h = helpers(5, null);
        Thread ran = h.call(Thread::currentThread, PLAIN, "http://a.net/");
        Assertions.assertNotSame(Thread.currentThread(), ran);
        Assertions.assertTrue(ran.getName().startsWith("test-helper-"), ran.getName());
        Assertions.assertTrue(ran.isDaemon());
        Assertions.assertEquals(1, h.largestPoolSize());
    }

    @Test
    void protocolExceptionsPropagateUnwrapped() {
        FetchTimeoutHelpers h = helpers(5, null);
        IOException thrown =
                Assertions.assertThrows(
                        IOException.class,
                        () ->
                                h.call(
                                        () -> {
                                            throw new IOException("boom");
                                        },
                                        PLAIN,
                                        "http://a.net/"));
        Assertions.assertEquals("boom", thrown.getMessage());
    }

    @Test
    void errorsAreWrappedInAnException() {
        FetchTimeoutHelpers h = helpers(5, null);
        Exception thrown =
                Assertions.assertThrows(
                        Exception.class,
                        () ->
                                h.call(
                                        () -> {
                                            throw new AssertionError("not an exception");
                                        },
                                        PLAIN,
                                        "http://a.net/"));
        Assertions.assertInstanceOf(AssertionError.class, thrown.getCause());
    }

    @Test
    void deadlineThrowsTypedTimeoutAndInterruptsTheHelper() throws Exception {
        FetchTimeoutHelpers h = helpers(1, null);
        AtomicBoolean interrupted = new AtomicBoolean();
        CountDownLatch done = new CountDownLatch(1);
        long start = System.currentTimeMillis();
        FetchTimeoutHelpers.TimeoutException thrown =
                Assertions.assertThrows(
                        FetchTimeoutHelpers.TimeoutException.class,
                        () ->
                                h.call(
                                        () -> {
                                            try {
                                                Thread.sleep(10_000);
                                            } catch (InterruptedException e) {
                                                interrupted.set(true);
                                            }
                                            done.countDown();
                                            return null;
                                        },
                                        PLAIN,
                                        "http://a.net/page"));
        Assertions.assertTrue(System.currentTimeMillis() - start < 3_000);
        Assertions.assertTrue(thrown.getMessage().contains("http://a.net/page"));
        // the helper is asked to stop, even though not every protocol honours it
        Assertions.assertTrue(done.await(5, TimeUnit.SECONDS));
        Assertions.assertTrue(interrupted.get());
    }

    @Test
    void saturationFailsFastWithTypedException() throws Exception {
        FetchTimeoutHelpers h = helpers(1, 2);
        Assertions.assertEquals(2, h.maxHelpers());
        StuckProtocol stuck = new StuckProtocol();
        // fill the two helpers with fetches that never return
        for (int i = 0; i < 2; i++) {
            Assertions.assertThrows(
                    FetchTimeoutHelpers.TimeoutException.class,
                    () -> h.call(() -> stuck.getProtocolOutput("http://a.net/", null), stuck, "u"));
        }
        long start = System.currentTimeMillis();
        Assertions.assertThrows(
                FetchTimeoutHelpers.SaturatedException.class,
                () -> h.call(() -> "never", stuck, "http://a.net/3"));
        Assertions.assertTrue(System.currentTimeMillis() - start < 500, "rejected immediately");
        Assertions.assertEquals(2, h.largestPoolSize());
    }

    @Test
    void poolBoundComesFromConfigOrDefault() {
        Assertions.assertEquals(4, helpers(1, null).maxHelpers());
        helpers.shutdown();
        Assertions.assertEquals(7, helpers(1, 7).maxHelpers());
        helpers.shutdown();
        // never less than one helper
        Assertions.assertEquals(1, helpers(1, 0).maxHelpers());
    }

    @Test
    void afterShutdownCallsAreRejected() {
        FetchTimeoutHelpers h = helpers(1, null);
        h.shutdown();
        Assertions.assertThrows(
                FetchTimeoutHelpers.SaturatedException.class,
                () -> h.call(() -> "x", PLAIN, "http://a.net/"));
    }
}
