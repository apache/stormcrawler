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
package org.apache.stormcrawler.solr.persistence;

import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.solr.Constants;
import org.apache.stormcrawler.solr.SolrConnection;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.URLPartitioner;
import org.apache.stormcrawler.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt {

    private static final Logger LOG = LoggerFactory.getLogger(StatusUpdaterBolt.class);

    private static final String BOLT_TYPE = "status";

    private static final String SolrMetadataPrefix = "solr.status.metadata.prefix";

    private static final String SolrStatusRoutingFieldParamName =
            Constants.PARAMPREFIX + "%s.routing.fieldname";

    private URLPartitioner partitioner;

    /** Store the key used for routing explicitly as a field in metadata * */
    private String fieldNameForRoutingKey = null;

    private String mdPrefix;

    private SolrConnection connection;

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {

        super.prepare(stormConf, context, collector);

        mdPrefix = ConfUtils.getString(stormConf, SolrMetadataPrefix, "metadata");

        partitioner = new URLPartitioner();
        partitioner.configure(stormConf);

        fieldNameForRoutingKey =
                ConfUtils.getString(
                        stormConf,
                        String.format(
                                Locale.ROOT,
                                StatusUpdaterBolt.SolrStatusRoutingFieldParamName,
                                BOLT_TYPE));

        if (StringUtils.isNotBlank(fieldNameForRoutingKey)) {
            if (fieldNameForRoutingKey.startsWith("metadata.")) {
                fieldNameForRoutingKey = fieldNameForRoutingKey.substring("metadata.".length());
                fieldNameForRoutingKey =
                        String.format(Locale.ROOT, "%s.%s", mdPrefix, fieldNameForRoutingKey);
            }
        }

        try {
            connection = SolrConnection.getConnection(stormConf, BOLT_TYPE);
        } catch (Exception e) {
            LOG.error("Can't connect to Solr: {}", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void store(
            String url, Status status, Metadata metadata, Optional<Date> nextFetch, Tuple t)
            throws Exception {

        SolrInputDocument doc = new SolrInputDocument();

        doc.setField("url", url);

        doc.setField("host", URLUtil.getHost(url));

        doc.setField("status", status.name());

        Iterator<String> keyIterator = metadata.keySet().iterator();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            String[] values = metadata.getValues(key);
            doc.setField(String.format(Locale.ROOT, "%s.%s", mdPrefix, key), values);
        }

        String partitionKey = partitioner.getPartition(url, metadata);
        if (partitionKey == null) {
            partitionKey = "_DEFAULT_";
        }

        // store routing key?
        if (StringUtils.isNotBlank(fieldNameForRoutingKey)) {
            doc.setField(fieldNameForRoutingKey, partitionKey);
        }

        if (nextFetch.isPresent()) {
            doc.setField("nextFetchDate", nextFetch.get());
        }

        connection.addAsync(doc);

        super.ack(t, url);
    }

    @Override
    public void cleanup() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                LOG.error("Can't close connection to Solr: {}", e);
            }
        }
    }
}
