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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.storm.task.OutputCollector;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.parse.ParsingTester;
import org.apache.stormcrawler.persistence.Status;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.parser.ParseContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xml.sax.ContentHandler;

/**
 * Checks that the ParserBolt stops waiting for a parse after parser.tika.timeout.
 *
 * @see <a href="https://github.com/apache/stormcrawler/issues/2097">#2097</a>
 */
class ParserBoltTimeoutTest extends ParsingTester {

    private static final byte[] CONTENT = "some text".getBytes(StandardCharsets.UTF_8);

    private static final String SLOW_URL = "https://example.org/slow.txt";

    /** a parse of SLOW_URL keeps producing output or ignores interrupts, as configured */
    private static class SlowParserBolt extends ParserBolt {
        volatile boolean ignoreInterrupts;
        volatile Thread parseThread;
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch finished = new CountDownLatch(1);

        @Override
        void parse(
                TikaInputStream tis,
                ContentHandler handler,
                org.apache.tika.metadata.Metadata md,
                ParseContext parseContext)
                throws Exception {
            parseThread = Thread.currentThread();
            String name = md.get(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY);
            if (!"/slow.txt".equals(name)) {
                super.parse(tis, handler, md, parseContext);
                return;
            }
            try {
                if (ignoreInterrupts) {
                    // like a parser which does not check for interrupts
                    while (release.getCount() > 0) {
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            // ignored on purpose
                        }
                    }
                } else {
                    char[] chars = "x".toCharArray();
                    handler.startDocument();
                    while (true) {
                        handler.characters(chars, 0, chars.length);
                    }
                }
            } finally {
                finished.countDown();
            }
        }
    }

    private SlowParserBolt slowBolt;

    @BeforeEach
    void setupParserBolt() {
        slowBolt = new SlowParserBolt();
        setupParserBolt(slowBolt);
    }

    @AfterEach
    void releaseParse() {
        slowBolt.release.countDown();
    }

    private void prepare(Long timeout) {
        Map<String, Object> conf = new HashMap<>();
        if (timeout != null) {
            conf.put(ParserBolt.PARSE_TIMEOUT_PARAM, timeout);
        }
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    private void assertTimedOut() {
        Assertions.assertTrue(output.getEmitted().isEmpty());
        List<List<Object>> status = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(1, status.size());
        Assertions.assertEquals(SLOW_URL, status.get(0).get(0));
        Assertions.assertEquals(Status.ERROR, status.get(0).get(2));
        Metadata metadata = (Metadata) status.get(0).get(1);
        Assertions.assertEquals(
                "parse timeout", metadata.getFirstValue(Constants.STATUS_ERROR_MESSAGE));
        Assertions.assertEquals(1, output.getAckedTuples().size());
    }

    @Test
    void parseProducingOutputIsStopped() throws Exception {
        prepare(200L);
        parse(SLOW_URL, CONTENT, new Metadata());

        assertTimedOut();
        // the handler throws at the next event once the thread is interrupted
        Assertions.assertTrue(slowBolt.finished.await(10, TimeUnit.SECONDS));
    }

    @Test
    void nextDocumentIsParsedAfterStuckParse() throws Exception {
        prepare(1000L);
        slowBolt.ignoreInterrupts = true;
        parse(SLOW_URL, CONTENT, new Metadata());
        assertTimedOut();
        Thread stuck = slowBolt.parseThread;
        Assertions.assertTrue(stuck.isAlive());

        parse("https://example.org/fast.txt", CONTENT, new Metadata());
        List<List<Object>> emitted = output.getEmitted();
        Assertions.assertEquals(1, emitted.size());
        Assertions.assertEquals("some text", emitted.get(0).get(3).toString().strip());
        Assertions.assertNotSame(stuck, slowBolt.parseThread);

        slowBolt.release.countDown();
        Assertions.assertTrue(slowBolt.finished.await(10, TimeUnit.SECONDS));
    }

    @Test
    void documentIsParsedUnderTimeout() throws IOException {
        prepare(10_000L);
        parse("https://example.org/fast.txt", CONTENT, new Metadata());

        Assertions.assertTrue(output.getEmitted(Constants.StatusStreamName).isEmpty());
        List<List<Object>> emitted = output.getEmitted();
        Assertions.assertEquals(1, emitted.size());
        Assertions.assertEquals("some text", emitted.get(0).get(3).toString().strip());
        Assertions.assertNotSame(Thread.currentThread(), slowBolt.parseThread);
    }

    @Test
    void parseErrorIsReportedUnderTimeout() throws IOException {
        prepare(10_000L);
        // not a valid PDF, the parser fails
        Metadata metadata = new Metadata();
        metadata.setValue("Content-Type", "application/pdf");
        byte[] content = "%PDF-1.4 broken".getBytes(StandardCharsets.UTF_8);
        parse("https://example.org/broken.pdf", content, metadata);

        List<List<Object>> status = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(1, status.size());
        Metadata md = (Metadata) status.get(0).get(1);
        Assertions.assertEquals("parse error", md.getFirstValue(Constants.STATUS_ERROR_MESSAGE));
    }

    @Test
    void textLimitAppliesUnderTimeout() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(ParserBolt.PARSE_TIMEOUT_PARAM, 10_000L);
        conf.put(ParserBolt.TEXT_MAX_LENGTH_PARAM, 100);
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        byte[] content = "word ".repeat(10_000).getBytes(StandardCharsets.UTF_8);
        parse("https://example.org/big.txt", content, new Metadata());

        List<List<Object>> emitted = output.getEmitted();
        Assertions.assertEquals(1, emitted.size());
        Assertions.assertEquals(100, emitted.get(0).get(3).toString().length());
        Metadata metadata = (Metadata) emitted.get(0).get(2);
        Assertions.assertEquals("true", metadata.getFirstValue(ParserBolt.TEXT_TRIMMED_KEY));
    }

    @Test
    void noTimeoutByDefault() throws IOException {
        prepare(null);
        parse("https://example.org/fast.txt", CONTENT, new Metadata());

        Assertions.assertEquals(1, output.getEmitted().size());
        // parsed on the executor thread itself
        Assertions.assertSame(Thread.currentThread(), slowBolt.parseThread);
    }
}
