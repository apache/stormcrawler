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
package org.apache.stormcrawler.json;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.bolt.JSoupParserBolt;
import org.apache.stormcrawler.parse.ParsingTester;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsoupFilterTest extends ParsingTester {

    @BeforeEach
    void setupParserBolt() {
        bolt = new JSoupParserBolt();
        setupParserBolt(bolt);
    }

    protected void prepareParserBolt(String configFile, Map<String, Object> parserConfig) {
        parserConfig.put("jsoup.filters.config.file", configFile);
        bolt.prepare(
                parserConfig, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @Test
    void testLDJsonExtraction() throws IOException {
        prepareParserBolt("test.jsoupfilters.json");
        parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html");
        Assertions.assertEquals(1, output.getEmitted().size());
        List<Object> parsedTuple = output.getEmitted().get(0);
        Metadata metadata = (Metadata) parsedTuple.get(2);
        Assertions.assertNotNull(metadata);
        String[] scripts = metadata.getValues("streetAddress");
        Assertions.assertNotNull(scripts);
    }

    @Test
    void testLinkFilter() throws IOException {
        prepareParserBolt("test.jsoupfilters.json");
        parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html");
        List<List<Object>> status = output.getEmitted("status");
        Assertions.assertEquals(31, status.size());
        List<Object> parsedTuple = status.get(0);
        parsedTuple.toArray();
    }
}
