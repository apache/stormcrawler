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

import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilters;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.MetadataTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLFilterBolt extends BaseRichBolt {

    public static final Logger LOG = LoggerFactory.getLogger(URLFilterBolt.class);

    private URLFilters urlFilters;

    protected OutputCollector collector;

    private final String filterConfigFile;

    private final boolean discoveredOnly;

    private static final String _s = org.apache.stormcrawler.Constants.StatusStreamName;

    /**
     * Relies on the file defined in urlfilters.config.file and applied to all tuples regardless of
     * status.
     */
    public URLFilterBolt() {
        this(false, null);
    }

    public URLFilterBolt(boolean discoveredOnly, String filterConfigFile) {
        this.discoveredOnly = discoveredOnly;
        this.filterConfigFile = filterConfigFile;
    }

    @Override
    public void execute(Tuple input) {
        // the input can come from the standard stream or the status one
        // we'll emit to whichever it came from
        String stream = input.getSourceStreamId();
        if (stream == null) {
            stream = Utils.DEFAULT_STREAM_ID;
        }

        // must have at least a URL and metadata, possibly a status
        String urlString = input.getStringByField("url");
        Metadata metadata = (Metadata) input.getValueByField("metadata");
        Status status = (Status) input.getValueByField("status");

        // not a status we want to filter
        if (discoveredOnly && !status.equals(Status.DISCOVERED)) {
            Values v = new Values(urlString, metadata, status);
            collector.emit(stream, v);
            collector.ack(input);
            return;
        }

        // the filters operate a bit differently from what they do in the crawl
        // in that they are done on the source URL and not the targets
        // we'll trick the depth value accordingly
        final Metadata tempMed = new Metadata();
        tempMed.setValue(MetadataTransfer.depthKeyName, "-1");
        String filtered = urlFilters.filter(null, tempMed, urlString);
        if (StringUtils.isBlank(filtered)) {
            LOG.debug("URL rejected: {}", urlString);
            collector.ack(input);
            return;
        }

        Values v = new Values(filtered, metadata, status);
        collector.emit(stream, v);
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields f = new Fields("url", "metadata", "status");
        declarer.declareStream(_s, f);
        declarer.declare(f);
    }

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        if (filterConfigFile != null) {
            try {
                urlFilters = new URLFilters(stormConf, filterConfigFile);
            } catch (IOException e) {
                throw new RuntimeException("Can't load filters from " + filterConfigFile);
            }
        } else {
            urlFilters = URLFilters.fromConf(stormConf);
        }
    }
}
