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

package org.apache.stormcrawler.metrics;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.metrics.v1.V1CounterMetric;
import org.apache.stormcrawler.metrics.v1.V1ReducedMetric;
import org.apache.stormcrawler.metrics.v2.V2CounterMetric;
import org.apache.stormcrawler.metrics.v2.V2HistogramReducedMetric;
import org.apache.stormcrawler.metrics.v2.V2MeterReducedMetric;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CrawlerMetricsTest {

    private TopologyContext context;

    @BeforeEach
    void setUp() {
        context = TestUtil.getMockedTopologyContext();
    }

    // --- registerCounter routing ---

    @Test
    void registerCounterDefaultReturnsV1() {
        Map<String, Object> conf = Map.of();
        ScopedCounter counter = CrawlerMetrics.registerCounter(context, conf, "test", 10);
        assertInstanceOf(V1CounterMetric.class, counter);
    }

    @Test
    void registerCounterV1ReturnsV1() {
        Map<String, Object> conf = Map.of(CrawlerMetrics.METRICS_VERSION_KEY, "v1");
        ScopedCounter counter = CrawlerMetrics.registerCounter(context, conf, "test", 10);
        assertInstanceOf(V1CounterMetric.class, counter);
    }

    @Test
    void registerCounterV2ReturnsV2() {
        Map<String, Object> conf = Map.of(CrawlerMetrics.METRICS_VERSION_KEY, "v2");
        ScopedCounter counter = CrawlerMetrics.registerCounter(context, conf, "test", 10);
        assertInstanceOf(V2CounterMetric.class, counter);
    }

    @Test
    void registerCounterBothReturnsDual() {
        Map<String, Object> conf = Map.of(CrawlerMetrics.METRICS_VERSION_KEY, "both");
        ScopedCounter counter = CrawlerMetrics.registerCounter(context, conf, "test", 10);
        assertInstanceOf(DualCounterMetric.class, counter);
    }

    // --- registerMeanMetric routing ---

    @Test
    void registerMeanMetricDefaultReturnsV1() {
        Map<String, Object> conf = Map.of();
        ScopedReducedMetric metric = CrawlerMetrics.registerMeanMetric(context, conf, "test", 10);
        assertInstanceOf(V1ReducedMetric.class, metric);
    }

    @Test
    void registerMeanMetricV2ReturnsHistogram() {
        Map<String, Object> conf = Map.of(CrawlerMetrics.METRICS_VERSION_KEY, "v2");
        ScopedReducedMetric metric = CrawlerMetrics.registerMeanMetric(context, conf, "test", 10);
        assertInstanceOf(V2HistogramReducedMetric.class, metric);
    }

    @Test
    void registerMeanMetricBothReturnsDual() {
        Map<String, Object> conf = Map.of(CrawlerMetrics.METRICS_VERSION_KEY, "both");
        ScopedReducedMetric metric = CrawlerMetrics.registerMeanMetric(context, conf, "test", 10);
        assertInstanceOf(DualReducedMetric.class, metric);
    }

    // --- registerPerSecMetric routing ---

    @Test
    void registerPerSecMetricDefaultReturnsV1() {
        Map<String, Object> conf = Map.of();
        ScopedReducedMetric metric = CrawlerMetrics.registerPerSecMetric(context, conf, "test", 10);
        assertInstanceOf(V1ReducedMetric.class, metric);
    }

    @Test
    void registerPerSecMetricV2ReturnsMeter() {
        Map<String, Object> conf = Map.of(CrawlerMetrics.METRICS_VERSION_KEY, "v2");
        ScopedReducedMetric metric = CrawlerMetrics.registerPerSecMetric(context, conf, "test", 10);
        assertInstanceOf(V2MeterReducedMetric.class, metric);
    }

    @Test
    void registerPerSecMetricBothReturnsDual() {
        Map<String, Object> conf = Map.of(CrawlerMetrics.METRICS_VERSION_KEY, "both");
        ScopedReducedMetric metric = CrawlerMetrics.registerPerSecMetric(context, conf, "test", 10);
        assertInstanceOf(DualReducedMetric.class, metric);
    }

    // --- registerSingleMeanMetric ---

    @Test
    void registerSingleMeanMetricReturnsConsumer() {
        Map<String, Object> conf = Map.of(CrawlerMetrics.METRICS_VERSION_KEY, "v2");
        assertNotNull(CrawlerMetrics.registerSingleMeanMetric(context, conf, "test", 10));
    }

    // --- registerCollectionMetric ---

    @Test
    void registerCollectionMetricReturnsConsumer() {
        Map<String, Object> conf = Map.of(CrawlerMetrics.METRICS_VERSION_KEY, "v2");
        assertNotNull(CrawlerMetrics.registerCollectionMetric(context, conf, "test", 10));
    }
}
