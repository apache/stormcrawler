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
            buffer.add(url, new Metadata());
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
                "urlbuffer.class",
                "org.apache.stormcrawler.persistence.urlbuffer.SimpleURLBuffer");
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
    void urlsWithAnUnexpectedSchemeAreNotEmitted() {
        StoredRowSpout spout = new StoredRowSpout("file:///etc/hosts");
        FileSpoutOutputCollectorMock collector = new FileSpoutOutputCollectorMock();
        spout.open(conf(), TestUtil.getMockedTopologyContext(), collector);
        spout.activate();
        // first call fills the buffer, second one emits from it
        spout.nextTuple();
        spout.nextTuple();
        Assertions.assertNull(
                collector.getTuple(),
                "the spout emitted a stored URL whose scheme is not in the configured list: "
                        + collector.getTuple());
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
}
