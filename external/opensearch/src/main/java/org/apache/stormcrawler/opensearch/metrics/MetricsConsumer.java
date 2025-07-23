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

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.opensearch.IndexCreation;
import org.apache.stormcrawler.opensearch.OpenSearchConnection;
import org.apache.stormcrawler.util.ConfUtils;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.core.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends metrics to an OpenSearch index. The OpenSearch details are set in the configuration; an
 * optional argument sets a date format to append to the index name.
 *
 * <pre>
 *   topology.metrics.consumer.register:
 *        - class: "org.apache.stormcrawler.opensearch.metrics.MetricsConsumer"
 *          parallelism.hint: 1
 *          argument: "yyyy-MM-dd"
 * </pre>
 */
public class MetricsConsumer implements IMetricsConsumer {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private static final String OSBoltType = "metrics";

    /** name of the index to use for the metrics (default : metrics) * */
    private static final String OSMetricsIndexNameParamName =
            "opensearch." + OSBoltType + ".index.name";

    private String indexName;

    private OpenSearchConnection connection;

    private String stormID;

    /** optional date format passed as argument, must be parsable as a SimpleDateFormat */
    private SimpleDateFormat dateFormat;

    @Override
    public void prepare(
            Map<String, Object> stormConf,
            Object registrationArgument,
            TopologyContext context,
            IErrorReporter errorReporter) {
        indexName = ConfUtils.getString(stormConf, OSMetricsIndexNameParamName, "metrics");
        stormID = context.getStormId();
        if (registrationArgument != null) {
            dateFormat = new SimpleDateFormat((String) registrationArgument, Locale.ROOT);
            LOG.info("Using date format {}", registrationArgument);
        }
        try {
            connection = OpenSearchConnection.getConnection(stormConf, OSBoltType);
        } catch (Exception e1) {
            LOG.error("Can't connect to OpenSearch", e1);
            throw new RuntimeException(e1);
        }

        // create a template if it doesn't exist
        try {
            IndexCreation.checkOrCreateIndexTemplate(connection.getClient(), OSBoltType, LOG);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup() {
        if (connection != null) connection.close();
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        final Date now = new Date();
        for (DataPoint dataPoint : dataPoints) {
            handleDataPoints(taskInfo, dataPoint.name, dataPoint.value, now);
        }
    }

    private void handleDataPoints(
            final TaskInfo taskInfo, final String nameprefix, final Object value, final Date now) {
        if (value instanceof Number) {
            indexDataPoint(taskInfo, now, nameprefix, ((Number) value).doubleValue());
        } else if (value instanceof Map) {
            for (Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
                String newnameprefix = nameprefix + "." + entry.getKey();
                handleDataPoints(taskInfo, newnameprefix, entry.getValue(), now);
            }
        } else if (value instanceof Collection) {
            for (Object collectionObj : (Collection<Object>) value) {
                handleDataPoints(taskInfo, nameprefix, collectionObj, now);
            }
        } else {
            LOG.warn("Found data point value {} of {}", nameprefix, value.getClass().toString());
        }
    }

    /**
     * Returns the name of the index that metrics will be written to.
     *
     * @return elastic index name
     */
    private String getIndexName(Date timestamp) {
        if (dateFormat == null) return indexName;

        StringBuilder sb = new StringBuilder(indexName);
        sb.append("-").append(dateFormat.format(timestamp));
        return sb.toString();
    }

    private void indexDataPoint(TaskInfo taskInfo, Date timestamp, String name, double value) {
        try {
            XContentBuilder builder = jsonBuilder().startObject();
            builder.field("stormId", stormID);
            builder.field("srcComponentId", taskInfo.srcComponentId);
            builder.field("srcTaskId", taskInfo.srcTaskId);
            builder.field("srcWorkerHost", taskInfo.srcWorkerHost);
            builder.field("srcWorkerPort", taskInfo.srcWorkerPort);
            builder.field("name", name);
            builder.field("value", value);
            builder.field("timestamp", timestamp);
            builder.endObject();

            IndexRequest indexRequest = new IndexRequest(getIndexName(timestamp)).source(builder);
            connection.addToProcessor(indexRequest);
        } catch (Exception e) {
            LOG.error("problem when building request for OpenSearch", e);
        }
    }
}
