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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.metrics.ScopedReducedMetric;
import org.junit.jupiter.api.Test;

/**
 * Covers the guard in {@link ProtocolMetrics} directly, independent of what the protocols let
 * through into the metadata, see issue #2099.
 */
class ProtocolMetricsTest {

    private final Map<String, Object> recorded = new HashMap<>();

    private final ScopedReducedMetric metric = scope -> value -> recorded.put(scope, value);

    @Test
    void numericValueIsRecordedUnderItsScope() {
        Metadata md = new Metadata();
        md.setValue("metrics.dns_time", "42");

        ProtocolMetrics.update(metric, md);

        assertEquals(Map.of("dns_time", 42L), recorded);
    }

    @Test
    void nonNumericValueIsSkippedAndTheOthersAreStillRecorded() {
        Metadata md = new Metadata();
        md.setValue("metrics.dns_time", "not-a-number");
        md.setValue("metrics.connect_time", "7");

        ProtocolMetrics.update(metric, md);

        assertEquals(Map.of("connect_time", 7L), recorded);
    }
}
