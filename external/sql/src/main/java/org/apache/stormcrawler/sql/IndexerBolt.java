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
package org.apache.stormcrawler.sql;

import static org.apache.stormcrawler.Constants.StatusStreamName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.indexing.AbstractIndexerBolt;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Stores URL and selected metadata into a SQL table * */
public class IndexerBolt extends AbstractIndexerBolt {

    private static final Logger LOG = LoggerFactory.getLogger(IndexerBolt.class);

    public static final String SQL_INDEX_TABLE_PARAM_NAME = "sql.index.table";

    private OutputCollector collector;

    private MultiCountMetric eventCounter;

    private Connection connection;

    private PreparedStatement preparedStmt;

    private String tableName;

    private Map<String, Object> conf;

    @Override
    public void prepare(
            Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        this.collector = collector;

        this.eventCounter = context.registerMetric("SQLIndexer", new MultiCountMetric(), 10);

        this.tableName = ConfUtils.getString(conf, SQL_INDEX_TABLE_PARAM_NAME);

        this.conf = conf;
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");

        // Distinguish the value used for indexing
        // from the one used for the status
        String normalisedurl = valueForURL(tuple);

        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        boolean keep = filterDocument(metadata);
        if (!keep) {
            eventCounter.scope("Filtered").incrBy(1);
            // treat it as successfully processed even if
            // we do not index it
            collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
            collector.ack(tuple);
            return;
        }

        try {

            // which metadata to display?
            Map<String, String[]> keyVals = filterMetadata(metadata);
            List<String> keys = new ArrayList<>(keyVals.keySet());

            String query = buildQuery(keys);

            if (connection == null) {
                try {
                    connection = SQLUtil.getConnection(conf);
                } catch (SQLException ex) {
                    LOG.error(ex.getMessage(), ex);
                    throw new RuntimeException(ex);
                }
            }

            LOG.debug("PreparedStatement => {}", query);

            // create the mysql insert PreparedStatement
            preparedStmt = connection.prepareStatement(query);
            // TODO store the text of the document?
            if (StringUtils.isNotBlank(fieldNameForText())) {
                // builder.field(fieldNameForText(), trimText(text));
            }

            // send URL as field?
            if (fieldNameForURL() != null) {
                preparedStmt.setString(1, normalisedurl);
            }

            // Set all metadata parameters
            for (int i = 0; i < keys.size(); i++) {
                insert(preparedStmt, i + 2, keys.get(i), keyVals);
            }
            // Execute the statement (single row insert)
            preparedStmt.executeUpdate();

            eventCounter.scope("Indexed").incrBy(1);

            collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
            collector.ack(tuple);
        } catch (Exception e) {
            // do not send to status stream so that it gets replayed
            LOG.error("Error inserting into SQL", e);
            collector.fail(tuple);
            if (connection != null) {
                // reset the connection
                try {
                    connection.close();
                } catch (SQLException e1) {
                }
                connection = null;
            }
        }
    }

    private void insert(
            PreparedStatement preparedStmt,
            int position,
            String label,
            Map<String, String[]> keyVals)
            throws SQLException {
        String[] values = keyVals.get(label);
        String value = "";
        if (values == null || values.length == 0) {
            LOG.info("No values found for label {}", label);
        } else if (values.length > 1) {
            LOG.info("More than one value found for label {}", label);
            value = values[0];
        } else {
            value = values[0];
        }
        preparedStmt.setString(position, value);
    }

    private String buildQuery(final List<String> keys) {
        final String columns = String.join(", ", keys);
        final String placeholders = keys.stream().map(k -> "?").collect(Collectors.joining(", "));

        final String updates =
                keys.stream()
                        .map(k -> String.format(Locale.ROOT, "%s=VALUES(%s)", k, k))
                        .collect(Collectors.joining(", "));

        // Build the ON DUPLICATE KEY UPDATE clause
        // If there are metadata keys, update them; otherwise, update the URL field to itself
        final String updateClause =
                updates.isEmpty()
                        ? String.format(
                                Locale.ROOT, "%s=VALUES(%s)", fieldNameForURL(), fieldNameForURL())
                        : updates;

        return String.format(
                Locale.ROOT,
                """
                            INSERT INTO %s (%s%s)
                            VALUES (?%s)
                            ON DUPLICATE KEY UPDATE %s
                            """,
                tableName,
                fieldNameForURL(),
                columns.isEmpty() ? "" : ", " + columns,
                placeholders.isEmpty() ? "" : ", " + placeholders,
                updateClause);
    }

    @Override
    public void cleanup() {
        SQLUtil.closeResource(preparedStmt, "prepared statement");
        SQLUtil.closeResource(connection, "connection");
    }
}
