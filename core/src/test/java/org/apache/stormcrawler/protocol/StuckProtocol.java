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

package org.apache.stormcrawler.protocol;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRules.RobotRulesMode;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;

/**
 * A protocol whose fetches hang for a long time and ignore interruption, like a client blocked on a
 * socket. Counts the fetches that actually started.
 */
public class StuckProtocol implements Protocol {

    public static final AtomicInteger STARTED = new AtomicInteger();

    /** Number of robots.txt lookups which actually hung. */
    public static final AtomicInteger ROBOTS_HUNG = new AtomicInteger();

    /** Hosts whose robots.txt lookup the fetcher reported as timed out: served at once after. */
    public static final Set<String> ROBOTS_TIMED_OUT = ConcurrentHashMap.newKeySet();

    public static final long HANG_MILLIS = 10_000;

    /** Configuration key: when true, robots.txt lookups hang as well. */
    public static final String HANG_ROBOTS_KEY = "stuck.protocol.hang.robots";

    private boolean hangRobots = false;

    @Override
    public void configure(Config conf) {
        hangRobots = ConfUtils.getBoolean(conf, HANG_ROBOTS_KEY, false);
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, Metadata metadata) throws Exception {
        STARTED.incrementAndGet();
        hang();
        return new ProtocolResponse("late".getBytes(StandardCharsets.UTF_8), 200, new Metadata());
    }

    @Override
    public BaseRobotRules getRobotRules(String url) {
        if (hangRobots && !ROBOTS_TIMED_OUT.contains(host(url))) {
            ROBOTS_HUNG.incrementAndGet();
            hang();
        }
        return new SimpleRobotRules(RobotRulesMode.ALLOW_ALL);
    }

    /** Like the HTTP protocols, remembers the failure so the host is not looked up again. */
    @Override
    public void robotRulesTimedOut(String url) {
        ROBOTS_TIMED_OUT.add(host(url));
    }

    private static String host(String url) {
        try {
            return new URL(url).getHost();
        } catch (Exception e) {
            return url;
        }
    }

    private static void hang() {
        long until = System.currentTimeMillis() + HANG_MILLIS;
        while (System.currentTimeMillis() < until) {
            try {
                Thread.sleep(until - System.currentTimeMillis());
            } catch (InterruptedException e) {
                // ignored on purpose: an interrupt does not free a blocked socket read either
            }
        }
    }

    @Override
    public void cleanup() {}
}
