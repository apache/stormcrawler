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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import org.apache.storm.metrics2.reporters.ScheduledStormReporter;
import org.apache.stormcrawler.opensearch.IndexCreation;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.util.ConfUtils;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storm V2 metrics reporter that writes metrics to an OpenSearch index with the same document
 * structure as the V1 {@link MetricsConsumer}. This allows existing OpenSearch dashboards to work
 * unchanged during migration from V1 to V2 metrics.
 *
 * <p>Configuration in storm.yaml:
 *
 * <pre>
 *   storm.metrics.reporters:
 *     - class: "org.apache.stormcrawler.opensearch.metrics.MetricsReporter"
 *       report.period: 10
 *       report.period.units: "SECONDS"
 * </pre>
 */
public class MetricsReporter extends ScheduledStormReporter {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsReporter.class);

    private static final String OSBoltType = "metrics";

    private static final String OSMetricsIndexNameParamName =
            "opensearch." + OSBoltType + ".index.name";

    private static final String DATE_FORMAT_KEY = "opensearch.metrics.date.format";

    private ScheduledReporter reporter;

    @Override
    public void prepare(
            MetricRegistry metricsRegistry,
            Map<String, Object> topoConf,
            Map<String, Object> reporterConf) {

        String indexName = ConfUtils.getString(topoConf, OSMetricsIndexNameParamName, "metrics");
        String stormId = (String) topoConf.getOrDefault("storm.id", "unknown");

        SimpleDateFormat dateFormat = null;
        String dateFormatStr = ConfUtils.getString(topoConf, DATE_FORMAT_KEY, null);
        if (dateFormatStr != null) {
            dateFormat = new SimpleDateFormat(dateFormatStr, Locale.ROOT);
        }

        OpenSearchConnection connection;
        try {
            connection = OpenSearchConnection.getConnection(topoConf, OSBoltType);
        } catch (Exception e) {
            LOG.error("Can't connect to OpenSearch", e);
            throw new RuntimeException(e);
        }

        try {
            IndexCreation.checkOrCreateIndexTemplate(connection.getClient(), OSBoltType, LOG);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        TimeUnit reportPeriodUnit = getReportPeriodUnit(reporterConf);
        long reportPeriod = getReportPeriod(reporterConf);

        reporter =
                new OpenSearchScheduledReporter(
                        metricsRegistry, indexName, stormId, dateFormat, connection);

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

    /**
     * Inner ScheduledReporter that writes Codahale metrics to OpenSearch in the same format as the
     * V1 {@link MetricsConsumer}.
     */
    private static class OpenSearchScheduledReporter extends ScheduledReporter {

        private final String indexName;
        private final String stormId;
        private final SimpleDateFormat dateFormat;
        private final OpenSearchConnection connection;

        OpenSearchScheduledReporter(
                MetricRegistry registry,
                String indexName,
                String stormId,
                SimpleDateFormat dateFormat,
                OpenSearchConnection connection) {
            super(
                    registry,
                    "opensearch-metrics-reporter",
                    MetricFilter.ALL,
                    TimeUnit.SECONDS,
                    TimeUnit.MILLISECONDS);
            this.indexName = indexName;
            this.stormId = stormId;
            this.dateFormat = dateFormat;
            this.connection = connection;
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
                } else if (value instanceof Map) {
                    for (Map.Entry<?, ?> mapEntry : ((Map<?, ?>) value).entrySet()) {
                        if (mapEntry.getValue() instanceof Number) {
                            indexDataPoint(
                                    now,
                                    entry.getKey() + "." + mapEntry.getKey(),
                                    ((Number) mapEntry.getValue()).doubleValue());
                        }
                    }
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

        private String getIndexName(Date timestamp) {
            if (dateFormat == null) {
                return indexName;
            }
            return indexName + "-" + dateFormat.format(timestamp);
        }

        private void indexDataPoint(Date timestamp, String name, double value) {
            try {
                Map<String, Object> doc = new HashMap<>();
                doc.put("stormId", stormId);
                doc.put("name", name);
                doc.put("value", value);
                doc.put("timestamp", timestamp.toInstant().toString());

                final String targetIndex = getIndexName(timestamp);
                BulkOperation op =
                        BulkOperation.of(b -> b.index(idx -> idx.index(targetIndex).document(doc)));
                connection.addToProcessor(op);
            } catch (Exception e) {
                LOG.error("Problem when building request for OpenSearch", e);
            }
        }
    }
}
