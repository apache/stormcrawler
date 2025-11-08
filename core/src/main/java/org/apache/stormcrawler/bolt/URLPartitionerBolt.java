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

package org.apache.stormcrawler.bolt;

import crawlercommons.domains.PaidLevelDomain;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Generates a partition key for a given URL based on the hostname, domain or IP address. */
public class URLPartitionerBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(URLPartitionerBolt.class);

    private OutputCollector collector;

    private MultiCountMetric eventCounter;

    private Map<String, String> cache;

    private String mode = Constants.PARTITION_MODE_HOST;

    @Override
    public void execute(Tuple tuple) {
        final String url = tuple.getStringByField("url");
        Metadata metadata = null;

        if (tuple.contains("metadata")) {
            metadata = (Metadata) tuple.getValueByField("metadata");
        }

        // maybe there is a field metadata but it can be null
        // or there was no field at all
        if (metadata == null) {
            metadata = Metadata.empty;
        }

        String partitionKey = null;
        String host = "";

        // IP in metadata?
        if (mode.equalsIgnoreCase(Constants.PARTITION_MODE_IP)) {
            String ipProvided = metadata.getFirstValue("ip");
            if (StringUtils.isNotBlank(ipProvided)) {
                partitionKey = ipProvided;
                eventCounter.scope("provided").incrBy(1);
            }
        }

        if (partitionKey == null) {
            URL u;
            try {
                u = new URL(url);
                host = u.getHost();
            } catch (MalformedURLException e1) {
                eventCounter.scope("Invalid URL").incrBy(1);
                LOG.warn("Invalid URL: {}", url);
                // ack it so that it doesn't get replayed
                collector.ack(tuple);
                return;
            }
        }

        // partition by hostname
        if (mode.equalsIgnoreCase(Constants.PARTITION_MODE_HOST)) {
            partitionKey = host;
        } else if (mode.equalsIgnoreCase(Constants.PARTITION_MODE_DOMAIN)) {
            // partition by domain : needs fixing
            partitionKey = PaidLevelDomain.getPLD(host);
        }

        // partition by IP
        if (mode.equalsIgnoreCase(Constants.PARTITION_MODE_IP) && partitionKey == null) {
            // try to get it from cache first
            partitionKey = cache.get(host);
            if (partitionKey != null) {
                eventCounter.scope("from cache").incrBy(1);
            } else {
                try {
                    long start = System.currentTimeMillis();
                    final InetAddress addr = InetAddress.getByName(host);
                    partitionKey = addr.getHostAddress();
                    long end = System.currentTimeMillis();
                    LOG.debug("Resolved IP {} in {} msec for : {}", partitionKey, end - start, url);

                    // add to cache
                    cache.put(host, partitionKey);

                } catch (final Exception e) {
                    eventCounter.scope("Unable to resolve IP").incrBy(1);
                    LOG.warn("Unable to resolve IP for: {}", host);
                    collector.ack(tuple);
                    return;
                }
            }
        }

        LOG.debug("Partition Key for: {} > {}", url, partitionKey);

        collector.emit(tuple, new Values(url, partitionKey, metadata));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "key", "metadata"));
    }

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {

        mode =
                ConfUtils.getString(
                        stormConf,
                        Constants.PARTITION_MODEParamName,
                        Constants.PARTITION_MODE_HOST);

        // check that the mode is known
        if (!mode.equals(Constants.PARTITION_MODE_IP)
                && !mode.equals(Constants.PARTITION_MODE_DOMAIN)
                && !mode.equals(Constants.PARTITION_MODE_HOST)) {
            LOG.error("Unknown partition mode : {} - forcing to byHost", mode);
            mode = Constants.PARTITION_MODE_HOST;
        }

        LOG.info("Using partition mode : {}", mode);

        this.collector = collector;
        // Register a "MultiCountMetric" to count different events in this bolt
        // Storm will emit the counts every n seconds to a special bolt via a
        // system stream
        // The data can be accessed by registering a "MetricConsumer" in the
        // topology
        this.eventCounter = context.registerMetric("URLPartitioner", new MultiCountMetric(), 10);

        final int maxEntries = 500;
        cache =
                new LinkedHashMap<>(maxEntries + 1, .75F, true) {
                    // This method is called just after a new entry has been added
                    @Override
                    public boolean removeEldestEntry(Map.Entry eldest) {
                        return size() > maxEntries;
                    }
                };

        // If the cache is to be used by multiple threads,
        // the cache must be wrapped with code to synchronize the methods
        cache = Collections.synchronizedMap(cache);
    }
}
