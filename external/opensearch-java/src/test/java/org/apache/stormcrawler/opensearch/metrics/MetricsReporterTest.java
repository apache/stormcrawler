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

package org.apache.stormcrawler.opensearch.metrics;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import java.util.HashMap;
import java.util.Map;
import org.apache.stormcrawler.opensearch.bolt.AbstractOpenSearchTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class MetricsReporterTest extends AbstractOpenSearchTest {

    @Test
    @Timeout(60)
    void prepareAndReportMetrics() {
        MetricRegistry registry = new MetricRegistry();
        Counter counter = registry.counter("test.counter");
        counter.inc(42);

        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(
                "opensearch.metrics.addresses",
                opensearchContainer.getHost() + ":" + opensearchContainer.getFirstMappedPort());

        Map<String, Object> reporterConf = new HashMap<>();
        reporterConf.put("report.period", 60L);
        reporterConf.put("report.period.units", "SECONDS");

        MetricsReporter reporter = new MetricsReporter();
        assertDoesNotThrow(() -> reporter.prepare(registry, topoConf, reporterConf));
        assertNotNull(reporter);
        reporter.stop();
    }
}
