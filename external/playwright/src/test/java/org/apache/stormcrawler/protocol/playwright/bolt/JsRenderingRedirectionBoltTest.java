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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestOutputCollector;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.playwright.HttpProtocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsRenderingRedirectionBoltTest {

    private JsRenderingRedirectionBolt bolt;
    private TestOutputCollector output;

    @BeforeEach
    void setUp() {
        bolt = new JsRenderingRedirectionBolt();
        output = new TestOutputCollector();
    }

    private void prepare(final Map<String, Object> conf) {
        final TopologyContext context = mock(TopologyContext.class);
        bolt.prepare(conf, context, new OutputCollector(output));
    }

    private Tuple tupleWith(final String url, final byte[] content, final Metadata metadata) {
        final Tuple t = mock(Tuple.class);
        when(t.getStringByField("url")).thenReturn(url);
        when(t.getBinaryByField("content")).thenReturn(content);
        when(t.getValueByField("metadata")).thenReturn(metadata);
        when(t.getStringByField("text")).thenReturn("some text");
        return t;
    }

    @Test
    void redirectsToStatusStreamWhenFlagPresent() {
        prepare(new HashMap<>());
        final Metadata md = new Metadata();
        md.setValue("fetch.with", "playwright");
        bolt.execute(tupleWith("http://example.com/", new byte[] {1, 2, 3}, md));

        final List<List<Object>> status = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(1, status.size(), "should redirect to status stream");
        Assertions.assertEquals("http://example.com/", status.get(0).get(0));
        Assertions.assertSame(md, status.get(0).get(1));
        Assertions.assertEquals(Status.FETCHED, status.get(0).get(2));

        Assertions.assertEquals(0, output.getEmitted().size(), "default stream should be empty");
        Assertions.assertEquals(1, output.getAckedTuples().size());
    }

    @Test
    void passesThroughWhenFlagAbsent() {
        prepare(new HashMap<>());
        final Metadata md = new Metadata();
        bolt.execute(tupleWith("http://example.com/", new byte[] {1, 2, 3}, md));

        final List<List<Object>> def = output.getEmitted();
        Assertions.assertEquals(1, def.size(), "should pass through default stream");
        Assertions.assertEquals(
                4, def.get(0).size(), "default emission has url/content/metadata/text");

        Assertions.assertEquals(
                0,
                output.getEmitted(Constants.StatusStreamName).size(),
                "status stream should be empty");
        Assertions.assertEquals(1, output.getAckedTuples().size());
    }

    @Test
    void skipsLoopWhenAlreadyFetchedByPlaywright() {
        prepare(new HashMap<>());
        // both flags set: detector flagged it AND it just came back from Playwright
        final Metadata md = new Metadata();
        md.setValue("fetch.with", "playwright");
        md.setValue(HttpProtocol.MD_KEY_END, "2026-05-04T00:00:00Z");
        bolt.execute(tupleWith("http://example.com/", new byte[] {1, 2, 3}, md));

        Assertions.assertEquals(1, output.getEmitted().size());
        Assertions.assertEquals(0, output.getEmitted(Constants.StatusStreamName).size());
    }

    @Test
    void honoursCustomMetadataKeyAndValue() {
        final Map<String, Object> conf = new HashMap<>();
        conf.put(JsRenderingRedirectionBolt.CONF_METADATA_KEY, "render");
        conf.put(JsRenderingRedirectionBolt.CONF_METADATA_VALUE, "yes");
        prepare(conf);

        final Metadata md = new Metadata();
        md.setValue("render", "yes");
        bolt.execute(tupleWith("http://example.com/", new byte[] {1}, md));

        Assertions.assertEquals(1, output.getEmitted(Constants.StatusStreamName).size());
        Assertions.assertEquals(0, output.getEmitted().size());
    }

    @Test
    void wrongValueDoesNotRedirect() {
        prepare(new HashMap<>());
        final Metadata md = new Metadata();
        // key set but to a different routing target
        md.setValue("fetch.with", "selenium");
        bolt.execute(tupleWith("http://example.com/", new byte[] {1}, md));

        Assertions.assertEquals(1, output.getEmitted().size());
        Assertions.assertEquals(0, output.getEmitted(Constants.StatusStreamName).size());
    }

    @Test
    void emptySkipKeyDisablesLoopGuard() {
        final Map<String, Object> conf = new HashMap<>();
        conf.put(JsRenderingRedirectionBolt.CONF_SKIP_IF_METADATA_PRESENT, "");
        prepare(conf);

        final Metadata md = new Metadata();
        md.setValue("fetch.with", "playwright");
        md.setValue(HttpProtocol.MD_KEY_END, "2026-05-04T00:00:00Z");
        bolt.execute(tupleWith("http://example.com/", new byte[] {1}, md));

        // loop guard disabled → still redirects despite the playwright marker
        Assertions.assertEquals(1, output.getEmitted(Constants.StatusStreamName).size());
        Assertions.assertEquals(0, output.getEmitted().size());
    }
}
