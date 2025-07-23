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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.sql.Constants;
import org.apache.stormcrawler.sql.SQLUtil;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsConsumer implements IMetricsConsumer {

    private final Logger LOG = LoggerFactory.getLogger(getClass());
    private Connection connection;
    private String query;

    @Override
    public void prepare(
            Map stormConf,
            Object registrationArgument,
            TopologyContext context,
            IErrorReporter errorReporter) {
        final String tableName =
                ConfUtils.getString(stormConf, Constants.SQL_METRICS_TABLE_PARAM_NAME, "metrics");
        query =
                "INSERT INTO "
                        + tableName
                        + " (srcComponentId, srcTaskId, srcWorkerHost, srcWorkerPort, name, value, timestamp)"
                        + " values (?, ?, ?, ?, ?, ?, ?)";
        try {
            connection = SQLUtil.getConnection(stormConf);
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        final Timestamp now = Timestamp.from(Instant.now());

        try {
            PreparedStatement preparedStmt = connection.prepareStatement(query);
            for (DataPoint dataPoint : dataPoints) {
                handleDataPoints(preparedStmt, taskInfo, dataPoint.name, dataPoint.value, now);
            }
            preparedStmt.executeBatch();
            preparedStmt.close();
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    private void handleDataPoints(
            final PreparedStatement preparedStmt,
            final TaskInfo taskInfo,
            final String nameprefix,
            final Object value,
            final Timestamp now) {
        if (value instanceof Number) {
            try {
                indexDataPoint(
                        preparedStmt, taskInfo, now, nameprefix, ((Number) value).doubleValue());
            } catch (SQLException e) {
                LOG.error("Exception while indexing datapoint", e);
            }
        } else if (value instanceof Map) {
            for (Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
                String newnameprefix = nameprefix + "." + entry.getKey();
                handleDataPoints(preparedStmt, taskInfo, newnameprefix, entry.getValue(), now);
            }
        } else if (value instanceof Collection) {
            for (Object collectionObj : (Collection<Object>) value) {
                handleDataPoints(preparedStmt, taskInfo, nameprefix, collectionObj, now);
            }
        } else {
            LOG.warn("Found data point value {} of {}", nameprefix, value.getClass().toString());
        }
    }

    private void indexDataPoint(
            final PreparedStatement preparedStmt,
            TaskInfo taskInfo,
            Timestamp timestamp,
            String name,
            double value)
            throws SQLException {
        preparedStmt.setString(1, taskInfo.srcComponentId);
        preparedStmt.setInt(2, taskInfo.srcTaskId);
        preparedStmt.setString(3, taskInfo.srcWorkerHost);
        preparedStmt.setInt(4, taskInfo.srcWorkerPort);
        preparedStmt.setString(5, name);
        preparedStmt.setDouble(6, value);
        preparedStmt.setObject(7, timestamp);
        preparedStmt.addBatch();
    }

    @Override
    public void cleanup() {
        try {
            connection.close();
        } catch (SQLException e) {
        }
    }
}
