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

package org.apache.stormcrawler.parse;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.List;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.bolt.JSoupParserBolt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @see <a href="https://github.com/apache/stormcrawler/pull/653">PR #653</a>
 */
class StackOverflowTest extends ParsingTester {

    @BeforeEach
    void setupParserBolt() {
        bolt = new JSoupParserBolt();
        setupParserBolt(bolt);
    }

    @Test
    void testStackOverflow() throws IOException {
        prepareParserBolt("test.parsefilters.json");
        Metadata metadata = new Metadata();
        parse("https://stormcrawler.apache.org", "stackexception.html", metadata);
        Assertions.assertEquals(31, output.getEmitted(Constants.StatusStreamName).size());
    }

    /**
     * @see <a href="https://github.com/apache/stormcrawler/issues/666">Issue #666</a>
     */
    @Test
    void testNamespaceExtraction() throws IOException {
        prepareParserBolt("test.parsefilters.json");
        Metadata metadata = new Metadata();
        parse("https://stormcrawler.apache.org", "stackexception.html", metadata);
        Assertions.assertEquals(1, output.getEmitted().size());
        List<Object> obj = output.getEmitted().get(0);
        Metadata m = (Metadata) obj.get(2);
        assertNotNull(m.getFirstValue("title"));
    }
}
