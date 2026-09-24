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
import org.apache.stormcrawler.persistence.Status;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Proves parser.tika.timeout (Tika Pipes) kills a stuck parse outright, unlike cooperative
 * interruption: MockParser.hang(interruptible=false) never checks Thread.interrupt() and produces
 * no SAX events, so a callback-based interrupt check would never even run.
 *
 * <p>{@code <mock>} content needs an {@code <?xml ...?>} declaration: MockParser is dispatched via
 * {@code application/mock+xml} (registered as root-XML "mock" in tika-core's own
 * custom-mimetypes.xml), and root-XML sniffing only refines bytes already magic-classified as
 * {@code application/xml}. Without the declaration it falls back to text/plain.
 *
 * @see <a href="https://github.com/apache/stormcrawler/issues/2097">#2097</a>
 * @see <a href="https://github.com/apache/stormcrawler/pull/2182">#2182</a>
 */
class ParserBoltPipesTimeoutTest extends ParsingTester {

    private static final String XML_DECLARATION = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

    @BeforeEach
    void setupParserBolt() {
        bolt = new ParserBolt();
        setupParserBolt(bolt);
    }

    private void prepare(Map<String, Object> extraConf) {
        Map<String, Object> conf = new HashMap<>(extraConf);
        conf.putIfAbsent(ParserBolt.PARSE_TIMEOUT_PARAM, 5_000L);
        conf.putIfAbsent(ParserBolt.PIPES_JVM_ARGS_PARAM, "-Xmx256m");
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    /** A 2s timeout must stop the bolt well before the hang's own 60s duration elapses. */
    @Test
    @Timeout(30)
    void parserThatIgnoresInterruptsIsKilledByTimeout() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(ParserBolt.PARSE_TIMEOUT_PARAM, 2_000L);
        prepare(conf);

        String url = "https://example.org/hang.xml";
        byte[] content =
                (XML_DECLARATION + "<mock><hang millis=\"60000\" interruptible=\"false\"/></mock>")
                        .getBytes(StandardCharsets.UTF_8);

        long start = System.currentTimeMillis();
        parse(url, content, new Metadata());
        long elapsed = System.currentTimeMillis() - start;

        Assertions.assertTrue(
                elapsed < 20_000,
                "the bolt should not have waited anywhere near the hang's"
                        + " own 60s duration, took "
                        + elapsed
                        + "ms");

        Assertions.assertTrue(output.getEmitted().isEmpty());
        List<List<Object>> status = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(1, status.size());
        Assertions.assertEquals(url, status.get(0).get(0));
        Assertions.assertEquals(Status.ERROR, status.get(0).get(2));
        Metadata md = (Metadata) status.get(0).get(1);
        Assertions.assertEquals("parse timeout", md.getFirstValue(Constants.STATUS_ERROR_MESSAGE));
        Assertions.assertEquals(1, output.getAckedTuples().size());
    }

    /** Parses normally within the timeout: text and outlinks are still emitted. */
    @Test
    @Timeout(30)
    void documentIsParsedUnderTimeout() throws IOException {
        prepare(new HashMap<>());

        String url = "https://example.org/fast.html";
        byte[] content =
                ("<html><head><title>t</title></head><body><p>hello world</p>"
                                + "<a href=\"http://example.com/next\">next page</a>"
                                + "</body></html>")
                        .getBytes(StandardCharsets.UTF_8);
        parse(url, content, new Metadata());

        Assertions.assertTrue(
                output.getEmitted(Constants.StatusStreamName).stream()
                        .noneMatch(t -> t.get(2) == Status.ERROR));
        List<List<Object>> emitted = output.getEmitted();
        Assertions.assertEquals(1, emitted.size());
        Assertions.assertTrue(emitted.get(0).get(3).toString().contains("hello world"));
        Metadata parseMetadata = (Metadata) emitted.get(0).get(2);
        Assertions.assertEquals("t", parseMetadata.getFirstValue("parse.dc:title"));
        // the fork returns the content as metadata, it must not be copied to parse.*
        Assertions.assertNull(parseMetadata.getFirstValue("parse.tk:content"));
        Assertions.assertNull(parseMetadata.getFirstValue("parse.tk:content-handler-type"));

        List<List<Object>> discovered =
                output.getEmitted(Constants.StatusStreamName).stream()
                        .filter(t -> t.get(2) == Status.DISCOVERED)
                        .toList();
        Assertions.assertEquals(1, discovered.size());
        Assertions.assertEquals("http://example.com/next", discovered.get(0).get(0));
    }

    /**
     * The fork stops at parser.tika.text.maxlength and returns XML cut off mid-document: the
     * document is still emitted, trimmed, instead of failing on the truncated XML.
     */
    @Test
    @Timeout(30)
    void textIsTrimmedUnderTimeout() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(ParserBolt.TEXT_MAX_LENGTH_PARAM, 5);
        prepare(conf);

        String url = "https://example.org/long.html";
        byte[] content =
                ("<html><head><title>t</title></head><body><p>hello world</p>"
                                + "<p>more text after the limit</p></body></html>")
                        .getBytes(StandardCharsets.UTF_8);
        parse(url, content, new Metadata());

        Assertions.assertTrue(
                output.getEmitted(Constants.StatusStreamName).stream()
                        .noneMatch(t -> t.get(2) == Status.ERROR));
        List<List<Object>> emitted = output.getEmitted();
        Assertions.assertEquals(1, emitted.size());
        Assertions.assertFalse(emitted.get(0).get(3).toString().contains("more text"));
        Metadata parseMetadata = (Metadata) emitted.get(0).get(2);
        Assertions.assertEquals(
                "true", parseMetadata.getFirstValue(ParserBolt.TEXT_TRIMMED_KEY));
    }

    /** A forked JVM dying mid-parse is reported as "parse crash" and the bolt carries on. */
    @Test
    @Timeout(60)
    void crashedForkIsReportedUnderTimeout() throws IOException {
        prepare(new HashMap<>());

        String url = "https://example.org/crash.xml";
        byte[] content =
                (XML_DECLARATION + "<mock><system_exit/></mock>")
                        .getBytes(StandardCharsets.UTF_8);
        parse(url, content, new Metadata());

        List<List<Object>> status = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(1, status.size());
        Metadata md = (Metadata) status.get(0).get(1);
        Assertions.assertEquals("parse crash", md.getFirstValue(Constants.STATUS_ERROR_MESSAGE));

        // the fork restarts for the next document
        parse(
                "https://example.org/fast.html",
                "<html><body><p>hello again</p></body></html>".getBytes(StandardCharsets.UTF_8),
                new Metadata());
        List<List<Object>> emitted = output.getEmitted();
        Assertions.assertEquals(1, emitted.size());
        Assertions.assertTrue(emitted.get(0).get(3).toString().contains("hello again"));
    }

    /** A genuine parse failure (not a timeout or a crash) is still reported as "parse error". */
    @Test
    @Timeout(30)
    void parseErrorIsReportedUnderTimeout() throws IOException {
        prepare(new HashMap<>());

        String url = "https://example.org/broken.xml";
        byte[] content =
                (XML_DECLARATION
                                + "<mock><throw class=\"java.io.IOException\">broken on"
                                + " purpose</throw></mock>")
                        .getBytes(StandardCharsets.UTF_8);
        parse(url, content, new Metadata());

        List<List<Object>> status = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(1, status.size());
        Metadata md = (Metadata) status.get(0).get(1);
        Assertions.assertEquals("parse error", md.getFirstValue(Constants.STATUS_ERROR_MESSAGE));
    }

    /** parser.extract.embedded still gates embedded-document parsing under parser.tika.timeout. */
    @Test
    @Timeout(30)
    void embeddedNotParsedByDefaultUnderTimeout() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        conf.put("parser.extract.embedded", false);
        prepare(conf);

        parse(
                "https://stormcrawler.apache.org/test_recursive_embedded.docx",
                "test_recursive_embedded.docx");
        List<List<Object>> outTuples = output.getEmitted();
        Assertions.assertEquals(1, outTuples.size());
        Assertions.assertFalse(
                outTuples
                        .get(0)
                        .get(3)
                        .toString()
                        .contains("Life, Liberty and the pursuit of Happiness"),
                "embedded documents should not be parsed when parser.extract.embedded is false");
    }
}
