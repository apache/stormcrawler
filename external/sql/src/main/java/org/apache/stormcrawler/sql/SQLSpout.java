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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.AbstractQueryingSpout;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.StringTabScheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLSpout extends AbstractQueryingSpout {

    public static final Logger LOG = LoggerFactory.getLogger(SQLSpout.class);

    private static final Scheme SCHEME = new StringTabScheme();

    private static final String BASE_SQL =
            """
            SELECT *
            FROM (
                SELECT
                    rank() OVER (PARTITION BY host ORDER BY nextfetchdate DESC, url) AS ranking,
                    url,
                    metadata,
                    nextfetchdate
                FROM %s
                WHERE nextfetchdate <= ? %s
            ) AS urls_ranks
            WHERE urls_ranks.ranking <= ?
            ORDER BY ranking %s
            """;

    private static final String BUCKET_CLAUSE =
            """
            AND bucket = ?
            """;

    private static final String LIMIT_CLAUSE =
            """
            LIMIT ?
            """;

    private static final String URL_COLUMN = "url";
    private static final String METADATA_COLUMN = "metadata";

    private static String preparedSql;

    private Connection connection;
    private PreparedStatement ps;

    /**
     * if more than one instance of the spout exist, each one is in charge of a separate bucket
     * value. This is used to ensure a good diversity of URLs.
     */
    private int bucketNum = -1;

    /** Used to distinguish between instances in the logs * */
    protected String logIdprefix = "";

    private int maxDocsPerBucket;

    private int maxNumResults;

    private Instant lastNextFetchDate = null;

    @Override
    public void open(
            Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {

        super.open(conf, context, collector);

        maxDocsPerBucket = ConfUtils.getInt(conf, Constants.SQL_MAX_DOCS_BUCKET_PARAM_NAME, 5);

        final String tableName =
                ConfUtils.getString(conf, Constants.SQL_STATUS_TABLE_PARAM_NAME, "urls");

        maxNumResults = ConfUtils.getInt(conf, Constants.SQL_MAXRESULTS_PARAM_NAME, 100);

        try {
            connection = SQLUtil.getConnection(conf);
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }

        // determine bucket this spout instance will be in charge of
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        if (totalTasks > 1) {
            logIdprefix =
                    "[" + context.getThisComponentId() + " #" + context.getThisTaskIndex() + "] ";
            bucketNum = context.getThisTaskIndex();
        }

        final String bucketClause = (bucketNum >= 0) ? BUCKET_CLAUSE : "";
        final String limitClause = (maxNumResults != -1) ? LIMIT_CLAUSE : "";

        preparedSql = String.format(Locale.ROOT, BASE_SQL, tableName, bucketClause, limitClause);

        try {
            ps = connection.prepareStatement(preparedSql);
        } catch (SQLException e) {
            LOG.error("Failed to prepare statement", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(SCHEME.getOutputFields());
    }

    @Override
    protected void populateBuffer() {

        if (lastNextFetchDate == null) {
            lastNextFetchDate = Instant.now();
            lastTimeResetToNow = Instant.now();
        } else if (resetFetchDateAfterNSecs != -1) {
            Instant changeNeededOn =
                    Instant.ofEpochMilli(
                            lastTimeResetToNow.toEpochMilli() + (resetFetchDateAfterNSecs * 1000));
            if (Instant.now().isAfter(changeNeededOn)) {
                LOG.info(
                        "lastDate reset based on resetFetchDateAfterNSecs {}",
                        resetFetchDateAfterNSecs);
                lastNextFetchDate = Instant.now();
            }
        }

        // select entries from mysql
        // https://mariadb.com/kb/en/library/window-functions-overview/
        // http://www.mysqltutorial.org/mysql-window-functions/mysql-rank-function/

        int alreadyprocessed = 0;
        int numhits = 0;

        long timeStartQuery = System.currentTimeMillis();

        try {
            int i = 1;
            ps.setTimestamp(i++, new Timestamp(lastNextFetchDate.toEpochMilli()));

            if (bucketNum >= 0) {
                ps.setInt(i++, bucketNum);
            }

            ps.setInt(i++, maxDocsPerBucket);

            if (maxNumResults != -1) {
                ps.setInt(i++, maxNumResults);
            }

            // dump query to log
            LOG.debug("{} SQL query {}", logIdprefix, preparedSql);

            try (ResultSet rs = ps.executeQuery()) {
                final long timeTaken = recordQueryTiming(timeStartQuery);

                // iterate through the java resultset
                while (rs.next()) {
                    numhits++;
                    alreadyprocessed += processRow(rs);
                }

                postProcessResults(numhits, alreadyprocessed, timeTaken);
            }
        } catch (SQLException e) {
            LOG.error("Exception while querying table", e);
        }
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("{}  Ack for {}", logIdprefix, msgId);
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("{}  Fail for {}", logIdprefix, msgId);
        super.fail(msgId);
    }

    @Override
    public void close() {
        super.close();
        SQLUtil.closeResource(ps, "prepared statement");
        SQLUtil.closeResource(connection, "connection");
    }

    private long recordQueryTiming(long timeStartQuery) {
        long timeTaken = System.currentTimeMillis() - timeStartQuery;
        queryTimes.accept(timeTaken);
        return timeTaken;
    }

    private int processRow(final ResultSet rs) throws SQLException {

        final String url = rs.getString(URL_COLUMN);
        final String metadata = rs.getString(METADATA_COLUMN);

        // already processed? skip
        if (beingProcessed.containsKey(url)) {
            return 1;
        }

        final String normalisedMetadata =
                (metadata == null || metadata.startsWith("\t")) ? metadata : "\t" + metadata;

        final String urlWithMetadata = String.format(Locale.ROOT, "%s%s", url, normalisedMetadata);
        final List<Object> v =
                SCHEME.deserialize(
                        ByteBuffer.wrap(urlWithMetadata.getBytes(StandardCharsets.UTF_8)));
        buffer.add(url, (Metadata) v.get(1));

        return 0;
    }

    private void postProcessResults(
            final int numHits, final int alreadyProcessed, final long timeTaken) {

        // no results? reset the date
        if (numHits == 0) {
            lastNextFetchDate = null;
        }

        eventCounter.scope("already_being_processed").incrBy(alreadyProcessed);
        eventCounter.scope("queries").incrBy(1);
        eventCounter.scope("docs").incrBy(numHits);

        LOG.info(
                "{} SQL query returned {} hits in {} msec with {} already being processed",
                logIdprefix,
                numHits,
                timeTaken,
                alreadyProcessed);
    }
}
