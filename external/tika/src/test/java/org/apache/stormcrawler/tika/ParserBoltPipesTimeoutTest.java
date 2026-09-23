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
 * Checks that parser.tika.timeout, backed by Tika Pipes, actually stops a stuck parse instead of
 * merely asking it to stop. Uses Tika's own {@code MockParser} test fixture (from the tika-core
 * test-jar) to drive a parse that spins forever and explicitly ignores {@code
 * Thread.interrupt()}: the kind of parser a cooperative-interruption approach (checking the
 * interrupt flag from a SAX callback) cannot touch, since it is never reached. Killing the forked
 * process is the only thing that works here, and the point of this test is to prove that it does.
 *
 * <p>{@code MockParser} is dispatched to via {@code application/mock+xml}, which the tika-core
 * test-jar registers by {@code <root-XML localName="mock"/>} in its own {@code
 * custom-mimetypes.xml}. Root-XML sniffing only refines a document magic detection has already
 * classified as generic {@code application/xml}, so the {@code <mock>} content below must carry
 * an {@code <?xml ...?>} declaration -- without one, detection never gets past magic bytes and
 * falls back to {@code text/plain}. No content-type hint is needed once that declaration is
 * present.
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
        // a single, light forked JVM is enough for these tests and starts faster
        conf.putIfAbsent(ParserBolt.PIPES_NUM_CLIENTS_PARAM, 1);
        conf.putIfAbsent(ParserBolt.PIPES_JVM_ARGS_PARAM, "-Xmx256m");
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    /**
     * MockParser.hang(millis, interruptible=false) keeps sleeping through interruption for the
     * full duration: exactly the kind of parser PR #2182's SAX-callback interrupt check can never
     * reach, since it is not producing any SAX events at all. A short parser.tika.timeout must
     * still stop the bolt well before the hang's own (much longer) duration elapses.
     */
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
                elapsed < 20_000, "the bolt should not have waited anywhere near the hang's" + " own 60s duration, took " + elapsed + "ms");

        Assertions.assertTrue(output.getEmitted().isEmpty());
        List<List<Object>> status = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(1, status.size());
        Assertions.assertEquals(url, status.get(0).get(0));
        Assertions.assertEquals(Status.ERROR, status.get(0).get(2));
        Metadata md = (Metadata) status.get(0).get(1);
        Assertions.assertEquals("parse timeout", md.getFirstValue(Constants.STATUS_ERROR_MESSAGE));
        Assertions.assertEquals(1, output.getAckedTuples().size());
    }

    /** A document that parses well within the timeout is emitted normally, text and outlinks included. */
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

        Assertions.assertTrue(output.getEmitted(Constants.StatusStreamName).stream()
                .noneMatch(t -> t.get(2) == Status.ERROR));
        List<List<Object>> emitted = output.getEmitted();
        Assertions.assertEquals(1, emitted.size());
        Assertions.assertTrue(emitted.get(0).get(3).toString().contains("hello world"));

        List<List<Object>> discovered =
                output.getEmitted(Constants.StatusStreamName).stream()
                        .filter(t -> t.get(2) == Status.DISCOVERED)
                        .toList();
        Assertions.assertEquals(1, discovered.size());
        Assertions.assertEquals("http://example.com/next", discovered.get(0).get(0));
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
                outTuples.get(0).get(3).toString().contains("Life, Liberty and the pursuit of Happiness"),
                "embedded documents should not be parsed when parser.extract.embedded is false");
    }
}
