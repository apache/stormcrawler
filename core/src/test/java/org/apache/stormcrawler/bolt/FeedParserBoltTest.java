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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHeaders;
import org.apache.storm.task.OutputCollector;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.parse.ParsingTester;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FeedParserBoltTest extends ParsingTester {

    @BeforeEach
    void setupParserBolt() {
        bolt = new FeedParserBolt();
        setupParserBolt(bolt);
    }

    private void checkOutput() {
        Assertions.assertEquals(7, output.getEmitted(Constants.StatusStreamName).size());
        List<Object> fields = output.getEmitted(Constants.StatusStreamName).get(0);
        Assertions.assertEquals(3, fields.size());
    }

    @Test
    void testFeedParsing() throws IOException {
        prepareParserBolt("test.parsefilters.json");
        Metadata metadata = new Metadata();
        // specify that it is a Feed file
        metadata.setValue(FeedParserBolt.isFeedKey, "true");
        parse("https://stormcrawler.apache.org/rss.xml", "stormcrawler.rss", metadata);
        checkOutput();
    }

    @Test
    void testFeedParsingNoMT() throws IOException {
        Map parserConfig = new HashMap();
        parserConfig.put("feed.sniffContent", true);
        parserConfig.put("parsefilters.config.file", "test.parsefilters.json");
        parserConfig.put(ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "http.");
        bolt.prepare(
                parserConfig, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        Metadata metadata = new Metadata();
        // set mime-type
        metadata.setValue("http." + HttpHeaders.CONTENT_TYPE, "application/rss+xml");
        parse("https://stormcrawler.apache.org/rss.xml", "stormcrawler.rss", metadata);
        checkOutput();
    }

    @Test
    void testFeedParsingDetextBytes() throws IOException {
        Map parserConfig = new HashMap();
        parserConfig.put("feed.sniffContent", true);
        parserConfig.put("parsefilters.config.file", "test.parsefilters.json");
        bolt.prepare(
                parserConfig, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        Metadata metadata = new Metadata();
        parse("https://stormcrawler.apache.org/rss.xml", "stormcrawler.rss", metadata);
        checkOutput();
    }

    @Test
    void testNonFeedParsing() throws IOException {
        prepareParserBolt("test.parsefilters.json");
        // do not specify that it is a feed file
        parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html", new Metadata());
        Assertions.assertEquals(1, output.getEmitted().size());
    }
}
