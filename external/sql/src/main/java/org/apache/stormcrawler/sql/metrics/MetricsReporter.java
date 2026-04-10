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

package org.apache.stormcrawler.sql.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import org.apache.storm.metrics2.reporters.ScheduledStormReporter;
import org.apache.stormcrawler.sql.Constants;
import org.apache.stormcrawler.sql.SQLUtil;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storm V2 metrics reporter that writes metrics to a SQL table with the same schema as the V1
 * {@link MetricsConsumer}.
 *
 * <p>Configuration in storm.yaml:
 *
 * <pre>
 *   storm.metrics.reporters:
 *     - class: "org.apache.stormcrawler.sql.metrics.MetricsReporter"
 *       report.period: 10
 *       report.period.units: "SECONDS"
 * </pre>
 */
public class MetricsReporter extends ScheduledStormReporter {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsReporter.class);

    private ScheduledReporter reporter;

    @Override
    public void prepare(
            MetricRegistry metricsRegistry,
            Map<String, Object> topoConf,
            Map<String, Object> reporterConf) {

        String tableName =
                ConfUtils.getString(topoConf, Constants.SQL_METRICS_TABLE_PARAM_NAME, "metrics");

        Connection connection;
        try {
            connection = SQLUtil.getConnection(topoConf);
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }

        String query =
                "INSERT INTO " + tableName + " (name, value, timestamp)" + " values (?, ?, ?)";

        TimeUnit reportPeriodUnit = getReportPeriodUnit(reporterConf);
        long reportPeriod = getReportPeriod(reporterConf);

        reporter = new SQLScheduledReporter(metricsRegistry, connection, query);
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

    private static class SQLScheduledReporter extends ScheduledReporter {

        private final Connection connection;
        private final String query;

        SQLScheduledReporter(MetricRegistry registry, Connection connection, String query) {
            super(
                    registry,
                    "sql-metrics-reporter",
                    MetricFilter.ALL,
                    TimeUnit.SECONDS,
                    TimeUnit.MILLISECONDS);
            this.connection = connection;
            this.query = query;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public void report(
                SortedMap<String, Gauge> gauges,
                SortedMap<String, Counter> counters,
                SortedMap<String, Histogram> histograms,
                SortedMap<String, Meter> meters,
                SortedMap<String, Timer> timers) {

            Timestamp now = Timestamp.from(Instant.now());

            try {
                PreparedStatement preparedStmt = connection.prepareStatement(query);

                for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                    Object value = entry.getValue().getValue();
                    if (value instanceof Number) {
                        addDataPoint(
                                preparedStmt, now, entry.getKey(), ((Number) value).doubleValue());
                    }
                }

                for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                    addDataPoint(preparedStmt, now, entry.getKey(), entry.getValue().getCount());
                }

                for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                    addDataPoint(
                            preparedStmt,
                            now,
                            entry.getKey(),
                            entry.getValue().getSnapshot().getMean());
                }

                for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                    addDataPoint(
                            preparedStmt, now, entry.getKey(), entry.getValue().getOneMinuteRate());
                }

                for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                    addDataPoint(
                            preparedStmt,
                            now,
                            entry.getKey(),
                            entry.getValue().getSnapshot().getMean());
                }

                preparedStmt.executeBatch();
                preparedStmt.close();
            } catch (SQLException ex) {
                LOG.error(ex.getMessage(), ex);
            }
        }

        private void addDataPoint(
                PreparedStatement preparedStmt, Timestamp timestamp, String name, double value)
                throws SQLException {
            preparedStmt.setString(1, name);
            preparedStmt.setDouble(2, value);
            preparedStmt.setObject(3, timestamp);
            preparedStmt.addBatch();
        }
    }
}
