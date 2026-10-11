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

import crawlercommons.domains.PaidLevelDomain;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.bolt.FetcherBolt;
import org.apache.stormcrawler.util.URLUtil;
import org.slf4j.LoggerFactory;

/**
 * This class described the item to be fetched.
 *
 * <p>For internal use only. Not part of StormCrawler's public API.
 */
public final class FetchItem {
    // the bolt's category: log configurations for FetcherBolt keep covering these lines
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FetcherBolt.class);

    private final String queueId;
    private final String url;
    private final Tuple tuple;
    private final long creationTime;

    private FetchItem(String url, Tuple t, String queueId) {
        this.url = url;
        this.queueId = queueId;
        this.tuple = t;
        this.creationTime = System.currentTimeMillis();
    }

    /**
     * Create an item. Queue id will be created based on <code>queueMode</code> argument, either as
     * a protocol + hostname pair, protocol + IP address pair or protocol+domain pair.
     */
    static FetchItem create(URL u, String url, Tuple t, String queueMode) {

        String queueId;

        String key = null;
        // reuse any key that might have been given
        // be it the hostname, domain or IP
        if (t.contains("key")) {
            key = t.getStringByField("key");
        }
        if (StringUtils.isNotBlank(key)) {
            queueId = key.toLowerCase(Locale.ROOT);
            return new FetchItem(url, t, queueId);
        }

        // one canonical host for all queue modes: aliases of one server
        // (percent-escaping, case, trailing dot) must share a queue
        final String canonicalHost = URLUtil.getCanonicalHost(u);

        if (FetchItemQueues.QUEUE_MODE_IP.equalsIgnoreCase(queueMode)) {
            try {
                final InetAddress addr = InetAddress.getByName(canonicalHost);
                key = addr.getHostAddress();
            } catch (final UnknownHostException e) {
                LOG.warn("Unable to resolve IP for {}, using hostname as key.", canonicalHost);
                key = canonicalHost;
            }
        } else if (FetchItemQueues.QUEUE_MODE_DOMAIN.equalsIgnoreCase(queueMode)) {
            key = PaidLevelDomain.getPLD(canonicalHost);
            if (key == null) {
                LOG.warn("Unknown domain for url: {}, using hostname as key", url);
                key = canonicalHost;
            }
        } else {
            key = canonicalHost;
        }

        if (key == null) {
            LOG.warn("Unknown host for url: {}, using URL string as key", url);
            key = u.toExternalForm();
        }

        queueId = key.toLowerCase(Locale.ROOT);
        return new FetchItem(url, t, queueId);
    }

    public String queueId() {
        return queueId;
    }

    public String url() {
        return url;
    }

    public Tuple tuple() {
        return tuple;
    }

    /** When the item was created, in epoch milliseconds. */
    public long creationTime() {
        return creationTime;
    }
}
