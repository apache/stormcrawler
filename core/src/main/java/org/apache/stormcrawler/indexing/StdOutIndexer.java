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

package org.apache.stormcrawler.indexing;

import static org.apache.stormcrawler.Constants.StatusStreamName;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.Status;

/**
 * Indexer which generates fields for indexing and sends them to the standard output. Useful for
 * debugging and as an illustration of what AbstractIndexerBolt provides.
 */
public class StdOutIndexer extends AbstractIndexerBolt {
    OutputCollector collector;

    @Override
    public void prepare(
            Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("url");

        // Distinguish the value used for indexing
        // from the one used for the status
        String normalisedurl = valueForURL(tuple);

        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // should this document be kept?
        boolean keep = filterDocument(metadata);
        if (!keep) {
            // treat it as successfully processed even if
            // we do not index it
            collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
            collector.ack(tuple);
            return;
        }

        // display text of the document?
        if (StringUtils.isNotBlank(fieldNameForText())) {
            String text = tuple.getStringByField("text");
            System.out.println(fieldNameForText() + "\t" + trimValue(text));
        }

        if (StringUtils.isNotBlank(fieldNameForURL())) {
            System.out.println(fieldNameForURL() + "\t" + trimValue(normalisedurl));
        }

        // which metadata to display?
        Map<String, String[]> keyVals = filterMetadata(metadata);

        for (Map.Entry<String, String[]> entry : keyVals.entrySet()) {
            String[] values = entry.getValue();
            for (String value : values) {
                System.out.println(entry.getKey() + "\t" + trimValue(value));
            }
        }

        collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
        collector.ack(tuple);
    }

    private String trimValue(String value) {
        if (value.length() > 100) {
            return value.length() + " chars";
        }
        return value;
    }
}
