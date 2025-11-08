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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.URLPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Status updater for SQL backend. Discovered URLs are sent as a batch, whereas updates are atomic.
 */
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt {

    public static final Logger LOG = LoggerFactory.getLogger(StatusUpdaterBolt.class);

    private static final Timestamp NEVER = Timestamp.valueOf("3000-01-01 00:00:00");

    private MultiCountMetric eventCounter;

    private Connection connection;
    private String tableName;

    private URLPartitioner partitioner;
    private int maxNumBuckets = -1;

    private int batchMaxSize = 1000;
    private float batchMaxIdleMsec = 2000;

    private int currentBatchSize = 0;

    private PreparedStatement insertPreparedStmt = null;

    private long lastInsertBatchTime = -1;

    private String updateQuery;
    private String insertQuery;

    private final Map<String, List<Tuple>> waitingAck = new HashMap<>();

    public StatusUpdaterBolt(int maxNumBuckets) {
        this.maxNumBuckets = maxNumBuckets;
    }

    /** Does not shard based on the total number of queues * */
    public StatusUpdaterBolt() {}

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);

        this.eventCounter = context.registerMetric("counter", new MultiCountMetric(), 10);

        tableName = ConfUtils.getString(stormConf, Constants.SQL_STATUS_TABLE_PARAM_NAME, "urls");

        batchMaxSize =
                ConfUtils.getInt(stormConf, Constants.SQL_UPDATE_BATCH_SIZE_PARAM_NAME, 1000);

        try {
            connection = SQLUtil.getConnection(stormConf);
        } catch (SQLException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }

        String query =
                tableName
                        + " (url, status, nextfetchdate, metadata, bucket, host)"
                        + " values (?, ?, ?, ?, ?, ?)";

        updateQuery = "REPLACE INTO " + query;
        insertQuery = "INSERT IGNORE INTO " + query;

        try {
            insertPreparedStmt = connection.prepareStatement(insertQuery);
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(
                () -> {
                    try {
                        checkExecuteBatch();
                    } catch (SQLException ex) {
                        LOG.error(ex.getMessage(), ex);
                        throw new RuntimeException(ex);
                    }
                },
                0,
                1,
                TimeUnit.SECONDS);
    }

    @Override
    public synchronized void store(
            String url, Status status, Metadata metadata, Optional<Date> nextFetch, Tuple t)
            throws Exception {
        // check whether the batch needs sending
        checkExecuteBatch();

        final boolean isUpdate = !status.equals(Status.DISCOVERED);

        // already have an entry for this DISCOVERED URL
        if (!isUpdate && waitingAck.containsKey(url)) {
            List<Tuple> list = waitingAck.get(url);
            // add the tuple to the list for that url
            list.add(t);
            return;
        }

        final StringBuilder mdAsString = new StringBuilder();
        for (String mdKey : metadata.keySet()) {
            String[] vals = metadata.getValues(mdKey);
            for (String v : vals) {
                mdAsString.append("\t").append(mdKey).append("=").append(v);
            }
        }

        int partition = 0;
        String partitionKey = partitioner.getPartition(url, metadata);
        if (maxNumBuckets > 1) {
            // determine which shard to send to based on the host / domain /
            // IP
            partition = Math.abs(partitionKey.hashCode() % maxNumBuckets);
        }

        PreparedStatement preparedStmt = this.insertPreparedStmt;

        // create in table if does not already exist
        if (isUpdate) {
            preparedStmt = connection.prepareStatement(updateQuery);
        }

        preparedStmt.setString(1, url);
        preparedStmt.setString(2, status.toString());
        if (nextFetch.isPresent()) {
            final Timestamp tsp = Timestamp.from(nextFetch.get().toInstant());
            preparedStmt.setObject(3, tsp);
        } else {
            // a value so large it means it will never be refetched
            preparedStmt.setObject(3, NEVER);
        }
        preparedStmt.setString(4, mdAsString.toString());
        preparedStmt.setInt(5, partition);
        preparedStmt.setString(6, partitionKey);

        // updates are not batched
        if (isUpdate) {
            preparedStmt.executeUpdate();
            preparedStmt.close();
            eventCounter.scope("sql_updates_number").incrBy(1);
            super.ack(t, url);
            return;
        }

        // code below is for inserts i.e. DISCOVERED URLs
        preparedStmt.addBatch();

        if (lastInsertBatchTime == -1) {
            lastInsertBatchTime = System.currentTimeMillis();
        }

        // URL gets added to the cache in method ack
        // once this method has returned
        List<Tuple> ll = new java.util.ArrayList<>();
        ll.add(t);

        waitingAck.put(url, ll);

        currentBatchSize++;

        eventCounter.scope("sql_inserts_number").incrBy(1);
    }

    private synchronized void checkExecuteBatch() throws SQLException {
        if (currentBatchSize == 0) {
            return;
        }
        long now = System.currentTimeMillis();
        // check whether the insert batches need executing
        if ((currentBatchSize == batchMaxSize)) {
            LOG.info("About to execute batch - triggered by size");
        } else if (lastInsertBatchTime + (long) batchMaxIdleMsec < System.currentTimeMillis()) {
            LOG.info(
                    "About to execute batch - triggered by time. Due {}, now {}",
                    lastInsertBatchTime + (long) batchMaxIdleMsec,
                    now);
        } else {
            return;
        }

        try {
            long start = System.currentTimeMillis();
            insertPreparedStmt.executeBatch();
            long end = System.currentTimeMillis();

            LOG.info("Batched {} inserts executed in {} msec", currentBatchSize, end - start);
            waitingAck.forEach(
                    (k, v) -> {
                        for (Tuple t : v) {
                            super.ack(t, k);
                        }
                    });
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
            // fail the entire batch
            waitingAck.forEach(
                    (k, v) -> {
                        for (Tuple t : v) {
                            super.collector.fail(t);
                        }
                    });
        }

        lastInsertBatchTime = System.currentTimeMillis();
        currentBatchSize = 0;
        waitingAck.clear();

        insertPreparedStmt.close();
        insertPreparedStmt = connection.prepareStatement(insertQuery);
    }

    @Override
    public void cleanup() {
        if (connection != null)
            try {
                connection.close();
            } catch (SQLException e) {
            }
    }
}
