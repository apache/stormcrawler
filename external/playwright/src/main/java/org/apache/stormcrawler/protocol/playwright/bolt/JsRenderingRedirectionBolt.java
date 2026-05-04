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

package org.apache.stormcrawler.protocol.playwright.bolt;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.playwright.HttpProtocol;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.LoggerFactory;

/**
 * Bolt that consumes the routing flag set by {@link
 * org.apache.stormcrawler.protocol.playwright.parsefilter.JsRenderingDetector} (or any other
 * upstream component) and forces an immediate refetch through Playwright instead of letting the
 * cheap fetch's stub document propagate downstream.
 *
 * <p>Pipeline placement: between the parser bolt (which produces tuples of {@code (url, content,
 * metadata, text)}) and the indexer / persistence bolts. On hit, the bolt emits only to the {@link
 * Constants#StatusStreamName} with status {@link Status#FETCHED}, so the URL is rescheduled and the
 * stub never reaches the index. On miss, the tuple passes through to the default stream unchanged.
 *
 * <p>Pair this with a per-metadata-key fetch interval to control how soon the refetch happens — by
 * default {@code Status.FETCHED} reschedules at {@code fetchInterval.default} (24h):
 *
 * <pre>{@code
 * # refetch flagged URLs in 5 minutes rather than 24 hours
 * fetchInterval.fetch.with=playwright: 5
 * }</pre>
 *
 * <h3>Configuration</h3>
 *
 * <ul>
 *   <li>{@code playwright.redirect.metadata.key} (default {@code fetch.with})
 *   <li>{@code playwright.redirect.metadata.value} (default {@code playwright})
 *   <li>{@code playwright.redirect.skip.if.metadata.present} (default {@link
 *       HttpProtocol#MD_KEY_END}) — passes the tuple through unchanged when this metadata key is
 *       already set, preventing loops with content that came back from Playwright. Set to empty to
 *       disable the loop guard.
 * </ul>
 */
public class JsRenderingRedirectionBolt extends BaseRichBolt {

    private static final org.slf4j.Logger LOG =
            LoggerFactory.getLogger(JsRenderingRedirectionBolt.class);

    public static final String CONF_METADATA_KEY = "playwright.redirect.metadata.key";
    public static final String CONF_METADATA_VALUE = "playwright.redirect.metadata.value";
    public static final String CONF_SKIP_IF_METADATA_PRESENT =
            "playwright.redirect.skip.if.metadata.present";

    public static final String DEFAULT_METADATA_KEY = "fetch.with";
    public static final String DEFAULT_METADATA_VALUE = "playwright";

    private OutputCollector collector;
    private String routingKey;
    private String routingValue;
    private String skipIfMetadataPresent;

    @Override
    public void prepare(
            final Map<String, Object> conf,
            final TopologyContext context,
            final OutputCollector collector) {
        this.collector = collector;
        this.routingKey = ConfUtils.getString(conf, CONF_METADATA_KEY, DEFAULT_METADATA_KEY);
        this.routingValue = ConfUtils.getString(conf, CONF_METADATA_VALUE, DEFAULT_METADATA_VALUE);
        this.skipIfMetadataPresent =
                ConfUtils.getString(conf, CONF_SKIP_IF_METADATA_PRESENT, HttpProtocol.MD_KEY_END);
    }

    @Override
    public void execute(final Tuple tuple) {
        final String url = tuple.getStringByField("url");
        final byte[] content = tuple.getBinaryByField("content");
        final Metadata metadata = (Metadata) tuple.getValueByField("metadata");
        final String text = tuple.getStringByField("text");

        if (shouldRedirect(metadata)) {
            LOG.debug("Redirecting {} to Playwright (status stream)", url);
            collector.emit(
                    Constants.StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
        } else {
            collector.emit(tuple, new Values(url, content, metadata, text));
        }
        collector.ack(tuple);
    }

    private boolean shouldRedirect(final Metadata metadata) {
        if (metadata == null) {
            return false;
        }
        if (skipIfMetadataPresent != null
                && !skipIfMetadataPresent.isEmpty()
                && metadata.containsKey(skipIfMetadataPresent)) {
            // already came back from Playwright — don't loop
            return false;
        }
        return metadata.containsKeyWithValue(routingKey, routingValue);
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "content", "metadata", "text"));
        declarer.declareStream(Constants.StatusStreamName, new Fields("url", "metadata", "status"));
    }
}
