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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.solr.common.SolrInputDocument;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.solr.SolrConnection;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsConsumer implements IMetricsConsumer {

    private final Logger LOG = LoggerFactory.getLogger(MetricsConsumer.class);

    private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ROOT);

    private static final String BOLT_TYPE = "metrics";

    private static final String SolrTTLParamName = "solr.metrics.ttl";
    private static final String SolrTTLFieldParamName = "solr.metrics.ttl.field";

    private String ttlField;
    private String ttl;

    private SolrConnection connection;

    @Override
    public void prepare(
            Map<String, Object> stormConf,
            Object registrationArgument,
            TopologyContext topologyContext,
            IErrorReporter errorReporter) {

        ttlField = ConfUtils.getString(stormConf, SolrTTLFieldParamName, "__ttl__");
        ttl = ConfUtils.getString(stormConf, SolrTTLParamName, null);

        try {
            connection = SolrConnection.getConnection(stormConf, BOLT_TYPE);
        } catch (Exception e) {
            LOG.error("Can't connect to Solr: {}", e);
            throw new RuntimeException(e);
        }
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
            Iterator<Entry> keyValiter = ((Map) value).entrySet().iterator();
            while (keyValiter.hasNext()) {
                Entry entry = keyValiter.next();
                String newnameprefix = nameprefix + "." + entry.getKey();
                handleDataPoints(taskInfo, newnameprefix, entry.getValue(), now);
            }
        } else if (value instanceof Collection) {
            for (Object collectionObj : (Collection) value) {
                handleDataPoints(taskInfo, nameprefix, collectionObj, now);
            }
        } else {
            LOG.warn("Found data point value {} of {}", nameprefix, value.getClass().toString());
        }
    }

    private void indexDataPoint(TaskInfo taskInfo, Date timestamp, String name, double value) {
        try {
            SolrInputDocument doc = new SolrInputDocument();

            doc.addField("srcComponentId", taskInfo.srcComponentId);
            doc.addField("srcTaskId", taskInfo.srcTaskId);
            doc.addField("srcWorkerHost", taskInfo.srcWorkerHost);
            doc.addField("srcWorkerPort", taskInfo.srcWorkerPort);
            doc.addField("name", name);
            doc.addField("value", value);

            String ftmp = df.format(timestamp);
            doc.addField("timestamp", ftmp);

            if (this.ttl != null) {
                doc.addField(ttlField, ttl);
            }

            connection.addAsync(doc);
        } catch (Exception e) {
            LOG.error("Problem building a document to Solr", e);
        }
    }

    @Override
    public void cleanup() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                LOG.error("Can't close connection to Solr", e);
            }
        }
    }
}
