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

package org.apache.stormcrawler.tika;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.parse.ParsingTester;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Checks that the text extracted by the ParserBolt is limited by parser.tika.text.maxlength.
 *
 * @see <a href="https://github.com/apache/stormcrawler/issues/2097">#2097</a>
 */
class ParserBoltTextLimitTest extends ParsingTester {

    private static final byte[] CONTENT = "word ".repeat(10_000).getBytes(StandardCharsets.UTF_8);

    @BeforeEach
    void setupParserBolt() {
        bolt = new ParserBolt();
        setupParserBolt(bolt);
    }

    private void prepare(Integer maxLength) {
        Map<String, Object> conf = new HashMap<>();
        if (maxLength != null) {
            conf.put(ParserBolt.TEXT_MAX_LENGTH_PARAM, maxLength);
        }
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    @Test
    void textIsTrimmedAtLimit() throws IOException {
        prepare(100);
        parse("https://example.org/big.txt", CONTENT, new Metadata());

        Assertions.assertTrue(output.getEmitted(Constants.StatusStreamName).isEmpty());
        List<List<Object>> emitted = output.getEmitted();
        Assertions.assertEquals(1, emitted.size());
        String text = emitted.get(0).get(3).toString();
        Assertions.assertEquals(100, text.length());
        Assertions.assertTrue(text.startsWith("word word"));
        Metadata metadata = (Metadata) emitted.get(0).get(2);
        Assertions.assertEquals("true", metadata.getFirstValue(ParserBolt.TEXT_TRIMMED_KEY));
    }

    @Test
    void linksBeforeLimitAreKept() throws IOException {
        prepare(100);
        String html =
                "<html><body><a href=\"https://example.org/first\">first</a><p>"
                        + "word ".repeat(10_000)
                        + "</p></body></html>";
        parse(
                "https://example.org/big.html",
                html.getBytes(StandardCharsets.UTF_8),
                new Metadata());

        List<List<Object>> emitted = output.getEmitted();
        Assertions.assertEquals(1, emitted.size());
        Metadata metadata = (Metadata) emitted.get(0).get(2);
        Assertions.assertEquals("true", metadata.getFirstValue(ParserBolt.TEXT_TRIMMED_KEY));
        List<List<Object>> status = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(1, status.size());
        Assertions.assertEquals("https://example.org/first", status.get(0).get(0));
    }

    @Test
    void noLimitByDefault() throws IOException {
        prepare(null);
        parse("https://example.org/big.txt", CONTENT, new Metadata());

        List<List<Object>> emitted = output.getEmitted();
        Assertions.assertEquals(1, emitted.size());
        Assertions.assertTrue(emitted.get(0).get(3).toString().length() >= CONTENT.length - 1);
        Metadata metadata = (Metadata) emitted.get(0).get(2);
        Assertions.assertNull(metadata.getFirstValue(ParserBolt.TEXT_TRIMMED_KEY));
    }

    @Test
    void noLimitWithMinusOne() throws IOException {
        prepare(-1);
        parse("https://example.org/big.txt", CONTENT, new Metadata());

        List<List<Object>> emitted = output.getEmitted();
        Assertions.assertEquals(1, emitted.size());
        Assertions.assertTrue(emitted.get(0).get(3).toString().length() >= CONTENT.length - 1);
        Metadata metadata = (Metadata) emitted.get(0).get(2);
        Assertions.assertNull(metadata.getFirstValue(ParserBolt.TEXT_TRIMMED_KEY));
    }

    @Test
    void noLimitWithOtherNegativeValue() throws IOException {
        prepare(-2);
        parse("https://example.org/big.txt", CONTENT, new Metadata());

        Assertions.assertTrue(output.getEmitted(Constants.StatusStreamName).isEmpty());
        List<List<Object>> emitted = output.getEmitted();
        Assertions.assertEquals(1, emitted.size());
        Assertions.assertTrue(emitted.get(0).get(3).toString().length() >= CONTENT.length - 1);
        Metadata metadata = (Metadata) emitted.get(0).get(2);
        Assertions.assertNull(metadata.getFirstValue(ParserBolt.TEXT_TRIMMED_KEY));
    }
}
