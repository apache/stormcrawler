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
package org.apache.stormcrawler.solr.bolt;

import static org.apache.stormcrawler.Constants.StatusStreamName;

import java.util.Iterator;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.indexing.AbstractIndexerBolt;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.solr.SolrConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class IndexerBolt extends AbstractIndexerBolt {

    private static final Logger LOG = LoggerFactory.getLogger(IndexerBolt.class);

    private static final String BOLT_TYPE = "indexer";

    private OutputCollector _collector;

    private MultiCountMetric eventCounter;

    private SolrConnection connection;

    @Override
    public void prepare(
            Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);

        _collector = collector;

        try {
            connection = SolrConnection.getConnection(conf, BOLT_TYPE);
        } catch (Exception e) {
            LOG.error("Can't connect to Solr: {}", e);
            throw new RuntimeException(e);
        }

        this.eventCounter = context.registerMetric("SolrIndexerBolt", new MultiCountMetric(), 10);
    }

    @Override
    public void cleanup() {
        if (connection != null)
            try {
                connection.close();
            } catch (Exception e) {
                LOG.error("Can't close connection to Solr: {}", e);
            }
    }

    @Override
    public void execute(Tuple tuple) {

        String url = tuple.getStringByField("url");

        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        boolean keep = filterDocument(metadata);
        if (!keep) {
            eventCounter.scope("Filtered").incrBy(1);
            // treat it as successfully processed even if
            // we do not index it
            _collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
            _collector.ack(tuple);
            return;
        }

        try {
            SolrInputDocument doc = new SolrInputDocument();

            // index text content
            if (StringUtils.isNotBlank(fieldNameForText())) {
                final String text = trimText(tuple.getStringByField("text"));
                if (!ignoreEmptyFields() || StringUtils.isNotBlank(text)) {
                    doc.addField(fieldNameForText(), text);
                }
            }

            // url
            if (StringUtils.isNotBlank(fieldNameForURL())) {
                // Distinguish the value used for indexing
                // from the one used for the status
                String normalisedurl = valueForURL(tuple);
                doc.addField(fieldNameForURL(), normalisedurl);
            }

            // select which metadata to index
            Map<String, String[]> keyVals = filterMetadata(metadata);

            Iterator<String> iterator = keyVals.keySet().iterator();
            while (iterator.hasNext()) {
                String fieldName = iterator.next();
                String[] values = keyVals.get(fieldName);
                for (String value : values) {
                    if (!ignoreEmptyFields() || StringUtils.isNotBlank(value)) {
                        doc.addField(fieldName, value);
                    }
                }
            }

            connection.addAsync(doc);

            eventCounter.scope("Indexed").incrBy(1);

            _collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
            _collector.ack(tuple);

        } catch (Exception e) {
            LOG.error("Send update request to SOLR failed due to {}", e);
            _collector.fail(tuple);
        }
    }
}
