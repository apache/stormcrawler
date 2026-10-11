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

package org.apache.stormcrawler.persistence;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.spout.mocks.FileSpoutOutputCollectorMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * A row whose URL uses a scheme outside the configured {@code protocols} list must not be emitted:
 * URL filtering only runs on the discovery path, so the spout is the last place where the schemes
 * re-entering the topology from the store can be constrained.
 */
class AbstractQueryingSpoutSchemeTest {

    /** Minimal spout which returns whatever a backend row would contain. */
    private static class StoredRowSpout extends AbstractQueryingSpout {

        private final String url;

        StoredRowSpout(String url) {
            this.url = url;
        }

        @Override
        protected void populateBuffer() {
            Metadata stored = new Metadata();
            stored.setValue("stored.key", "stored.value");
            buffer.add(url, stored);
            markQueryReceivedNow();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("url", "metadata"));
        }
    }

    private static Map<String, Object> conf() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(
                "urlbuffer.class", "org.apache.stormcrawler.persistence.urlbuffer.SimpleURLBuffer");
        return conf;
    }

    @Test
    void httpUrlsFromTheBackendAreEmitted() {
        StoredRowSpout spout = new StoredRowSpout("https://example.com/page.html");
        FileSpoutOutputCollectorMock collector = new FileSpoutOutputCollectorMock();
        spout.open(conf(), TestUtil.getMockedTopologyContext(), collector);
        spout.activate();
        // first call fills the buffer, second one emits from it
        spout.nextTuple();
        spout.nextTuple();
        Assertions.assertNotNull(collector.getTuple());
        Assertions.assertEquals("https://example.com/page.html", collector.getTuple().get(0));
    }

    @Test
    void urlsWithAnUnexpectedSchemeAreNotEmittedButReported() {
        StoredRowSpout spout = new StoredRowSpout("file:///etc/hosts");
        FileSpoutOutputCollectorMock collector = new FileSpoutOutputCollectorMock();
        spout.open(conf(), TestUtil.getMockedTopologyContext(), collector);
        spout.activate();
        // first call fills the buffer, second one emits from it
        spout.nextTuple();
        spout.nextTuple();
        // the URL is not emitted on the default stream for fetching
        Assertions.assertNotEquals("default", collector.getStreamId());
        // it is reported on the status stream as ERROR, so the status updater
        // marks the row as ERROR in the store
        Assertions.assertEquals(Constants.StatusStreamName, collector.getStreamId());
        Assertions.assertEquals("file:///etc/hosts", collector.getTuple().get(0));
        Assertions.assertEquals(Status.ERROR, collector.getTuple().get(2));
        // the stored metadata is passed on, so the row is not overwritten with an empty set
        Assertions.assertEquals(
                "stored.value",
                ((Metadata) collector.getTuple().get(1)).getFirstValue("stored.key"));
        // the store records why the row is ERROR
        Assertions.assertNotNull(
                ((Metadata) collector.getTuple().get(1))
                        .getFirstValue(Constants.STATUS_ERROR_CAUSE),
                "the rejected row carries an error cause");
    }

    @Test
    void uppercaseSchemeIsAllowed() {
        StoredRowSpout spout = new StoredRowSpout("HTTPS://example.com/page.html");
        FileSpoutOutputCollectorMock collector = new FileSpoutOutputCollectorMock();
        spout.open(conf(), TestUtil.getMockedTopologyContext(), collector);
        spout.activate();
        spout.nextTuple();
        spout.nextTuple();
        Assertions.assertNotNull(collector.getTuple());
        Assertions.assertEquals("HTTPS://example.com/page.html", collector.getTuple().get(0));
    }

    /** The shipped protocols value is a comma-separated string, not a list. */
    @Test
    void shippedCommaSeparatedProtocolsAreSplitIntoSchemes() {
        Map<String, Object> conf = conf();
        conf.put("protocols", "http,https,file");
        FileSpoutOutputCollectorMock collector = new FileSpoutOutputCollectorMock();
        StoredRowSpout spout = new StoredRowSpout("https://example.com/page.html");
        spout.open(conf, TestUtil.getMockedTopologyContext(), collector);
        spout.activate();
        spout.nextTuple();
        spout.nextTuple();
        Assertions.assertNotNull(
                collector.getTuple(),
                "https must still be emitted when protocols is the shipped comma string");
    }

    @Test
    void schemeOutsideShippedCommaSeparatedProtocolsIsRejected() {
        Map<String, Object> conf = conf();
        conf.put("protocols", "http,https,file");
        FileSpoutOutputCollectorMock collector = new FileSpoutOutputCollectorMock();
        StoredRowSpout spout = new StoredRowSpout("ftp://example.com/file.txt");
        spout.open(conf, TestUtil.getMockedTopologyContext(), collector);
        spout.activate();
        spout.nextTuple();
        spout.nextTuple();
        Assertions.assertEquals(Constants.StatusStreamName, collector.getStreamId());
        Assertions.assertEquals(Status.ERROR, collector.getTuple().get(2));
    }

    /** The key may also be given as a YAML list - parsed the same way. */
    @Test
    void protocolsAsListIsHonoured() {
        Map<String, Object> conf = conf();
        conf.put("protocols", java.util.List.of("https"));
        FileSpoutOutputCollectorMock collector = new FileSpoutOutputCollectorMock();
        StoredRowSpout spout = new StoredRowSpout("http://example.com/page.html");
        spout.open(conf, TestUtil.getMockedTopologyContext(), collector);
        spout.activate();
        spout.nextTuple();
        spout.nextTuple();
        Assertions.assertEquals(Constants.StatusStreamName, collector.getStreamId());
        Assertions.assertEquals(Status.ERROR, collector.getTuple().get(2));
    }

    /** Whitespace around commas is tolerated, like ProtocolFactory does. */
    @Test
    void whitespaceInCommaSeparatedProtocolsIsTolerated() {
        Map<String, Object> conf = conf();
        conf.put("protocols", "http, https , file");
        FileSpoutOutputCollectorMock collector = new FileSpoutOutputCollectorMock();
        StoredRowSpout spout = new StoredRowSpout("https://example.com/page.html");
        spout.open(conf, TestUtil.getMockedTopologyContext(), collector);
        spout.activate();
        spout.nextTuple();
        spout.nextTuple();
        Assertions.assertNotNull(collector.getTuple());
    }
}
