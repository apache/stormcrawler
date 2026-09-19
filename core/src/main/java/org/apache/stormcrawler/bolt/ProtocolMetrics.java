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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.metrics.ScopedReducedMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies the timings a protocol reports under {@value #METRICS_PREFIX} into the metric registry of
 * a fetcher bolt. Shared by {@link FetcherBolt} and {@link SimpleFetcherBolt}.
 */
final class ProtocolMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolMetrics.class);

    /** Prefix of the metadata keys holding the timings reported by a protocol. */
    static final String METRICS_PREFIX = "metrics.";

    private static final AtomicBoolean nonNumericLogged = new AtomicBoolean();

    private ProtocolMetrics() {}

    /**
     * The protocols copy every response header into the response metadata, so a key under this
     * prefix can come from the fetched server rather than from the protocol itself. A value which
     * is not a number is skipped: it must not fail a fetch which otherwise completed.
     */
    static void update(ScopedReducedMetric averagedMetrics, Metadata metadata) {
        for (String key : metadata.keySet(METRICS_PREFIX)) {
            final String value = metadata.getFirstValue(key);
            final long parsed;
            try {
                parsed = Long.parseLong(value);
            } catch (NumberFormatException e) {
                if (nonNumericLogged.compareAndSet(false, true)) {
                    LOG.warn("Ignoring non-numeric value {} for {}", value, key);
                } else {
                    LOG.debug("Ignoring non-numeric value {} for {}", value, key);
                }
                continue;
            }
            averagedMetrics.scope(key.substring(METRICS_PREFIX.length())).update(parsed);
        }
    }
}
