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

package org.apache.stormcrawler.solr.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import org.apache.solr.common.SolrInputDocument;
import org.apache.storm.metrics2.reporters.ScheduledStormReporter;
import org.apache.stormcrawler.solr.SolrConnection;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storm V2 metrics reporter that writes metrics to Solr with the same document structure as the V1
 * {@link MetricsConsumer}.
 *
 * <p>Configuration in storm.yaml:
 *
 * <pre>
 *   storm.metrics.reporters:
 *     - class: "org.apache.stormcrawler.solr.metrics.MetricsReporter"
 *       report.period: 10
 *       report.period.units: "SECONDS"
 * </pre>
 */
public class MetricsReporter extends ScheduledStormReporter {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsReporter.class);

    private static final String BOLT_TYPE = "metrics";

    private static final String SolrTTLParamName = "solr.metrics.ttl";
    private static final String SolrTTLFieldParamName = "solr.metrics.ttl.field";

    private ScheduledReporter reporter;

    @Override
    public void prepare(
            MetricRegistry metricsRegistry,
            Map<String, Object> topoConf,
            Map<String, Object> reporterConf) {

        String ttlField = ConfUtils.getString(topoConf, SolrTTLFieldParamName, "__ttl__");
        String ttl = ConfUtils.getString(topoConf, SolrTTLParamName, null);

        SolrConnection connection;
        try {
            connection = SolrConnection.getConnection(topoConf, BOLT_TYPE);
        } catch (Exception e) {
            LOG.error("Can't connect to Solr: {}", e);
            throw new RuntimeException(e);
        }

        TimeUnit reportPeriodUnit = getReportPeriodUnit(reporterConf);
        long reportPeriod = getReportPeriod(reporterConf);

        reporter = new SolrScheduledReporter(metricsRegistry, connection, ttlField, ttl);
        reporter.start(reportPeriod, reportPeriodUnit);
    }

    @Override
    public void start() {
        // already started in prepare()
    }

    @Override
    public void stop() {
        if (reporter != null) {
            reporter.stop();
        }
    }

    private static class SolrScheduledReporter extends ScheduledReporter {

        private final SolrConnection connection;
        private final String ttlField;
        private final String ttl;
        private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT);

        SolrScheduledReporter(
                MetricRegistry registry, SolrConnection connection, String ttlField, String ttl) {
            super(
                    registry,
                    "solr-metrics-reporter",
                    MetricFilter.ALL,
                    TimeUnit.SECONDS,
                    TimeUnit.MILLISECONDS);
            this.connection = connection;
            this.ttlField = ttlField;
            this.ttl = ttl;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public void report(
                SortedMap<String, Gauge> gauges,
                SortedMap<String, Counter> counters,
                SortedMap<String, Histogram> histograms,
                SortedMap<String, Meter> meters,
                SortedMap<String, Timer> timers) {

            Date now = new Date();

            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                Object value = entry.getValue().getValue();
                if (value instanceof Number) {
                    indexDataPoint(now, entry.getKey(), ((Number) value).doubleValue());
                }
            }

            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                indexDataPoint(now, entry.getKey(), entry.getValue().getCount());
            }

            for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                indexDataPoint(now, entry.getKey(), entry.getValue().getSnapshot().getMean());
            }

            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                indexDataPoint(now, entry.getKey(), entry.getValue().getOneMinuteRate());
            }

            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                indexDataPoint(now, entry.getKey(), entry.getValue().getSnapshot().getMean());
            }
        }

        private void indexDataPoint(Date timestamp, String name, double value) {
            try {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField("name", name);
                doc.addField("value", value);
                doc.addField("timestamp", df.format(timestamp));

                if (ttl != null) {
                    doc.addField(ttlField, ttl);
                }

                connection.addAsync(doc);
            } catch (Exception e) {
                LOG.error("Problem building a document to Solr", e);
            }
        }
    }
}
