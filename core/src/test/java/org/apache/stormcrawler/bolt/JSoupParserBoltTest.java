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
import org.apache.stormcrawler.util.RobotsTags;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JSoupParserBoltTest extends ParsingTester {

    /*
     *
     * some sample tags:
     *
     * <meta name="robots" content="index,follow"> <meta name="robots"
     * content="noindex,follow"> <meta name="robots" content="index,nofollow">
     * <meta name="robots" content="noindex,nofollow">
     *
     * <META HTTP-EQUIV="Pragma" CONTENT="no-cache">
     */
    Map<String, Object> stormConf = new HashMap<>();

    public static String[] tests = {
        "<html><head><title>test page</title>"
                + "<META NAME=\"ROBOTS\" CONTENT=\"NONE\"> "
                + "</head><body>"
                + " some text"
                + "</body></html>",
        "<html><head><title>test page</title>"
                + "<meta name=\"robots\" content=\"all\"> "
                + "</head><body>"
                + " some text"
                + "</body></html>",
        "<html><head><title>test page</title>"
                + "<MeTa NaMe=\"RoBoTs\" CoNtEnT=\"nOnE\"> "
                + "</head><body>"
                + " some text"
                + "</body></html>",
        "<html><head><title>test page</title>"
                + "<meta name=\"robots\" content=\"none\"> "
                + "</head><body>"
                + " some text"
                + "</body></html>",
        "<html><head><title>test page</title>"
                + "<meta name=\"robots\" content=\"noindex,nofollow\"> "
                + "</head><body>"
                + " some text"
                + "</body></html>",
        "<html><head><title>test page</title>"
                + "<meta name=\"robots\" content=\"noindex,follow\"> "
                + "</head><body>"
                + " some text"
                + "</body></html>",
        "<html><head><title>test page</title>"
                + "<meta name=\"robots\" content=\"index,nofollow\"> "
                + "</head><body>"
                + " some text"
                + "</body></html>",
        "<html><head><title>test page</title>"
                + "<meta name=\"robots\" content=\"index,follow\"> "
                + "<base href=\"http://www.nutch.org/\">"
                + "</head><body>"
                + " some text"
                + "</body></html>",
        "<html><head><title>test page</title>"
                + "<meta name=\"robots\"> "
                + "<base href=\"http://www.nutch.org/base/\">"
                + "</head><body>"
                + " some text"
                + "</body></html>"
    };

    public static final boolean[][] answers = { // NONE
        {true, true, true}, // all
        {false, false, false}, // nOnE
        {true, true, true}, // none
        {true, true, true}, // noindex,nofollow
        {true, true, false}, // noindex,follow
        {true, false, false}, // index,nofollow
        {false, true, false}, // index,follow
        {false, false, false}, // missing!
        {false, false, false}
    };

    @BeforeEach
    void setupParserBolt() {
        bolt = new JSoupParserBolt();
        setupParserBolt(bolt);
    }

    /** Checks that content in script is not included in the text representation. */
    @Test
    void testNoScriptInText() throws IOException {
        bolt.prepare(
                new HashMap<>(), TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html");
        List<Object> parsedTuple = output.getEmitted().remove(0);
        // check in the metadata that the values match
        String text = (String) parsedTuple.get(3);
        Assertions.assertFalse(
                text.contains("urchinTracker"),
                "Text should not contain the content of script tags");
    }

    /** Checks that individual links marked as rel="nofollow" are not followed. */
    @Test
    void testNoFollowOutlinks() throws IOException {
        bolt.prepare(
                new HashMap<>(), TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html");
        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(25, statusTuples.size());
    }

    @Test
    void testHTTPRobots() throws IOException {
        bolt.prepare(
                new HashMap<>(), TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        Metadata metadata = new Metadata();
        metadata.setValues("X-Robots-Tag", new String[] {"noindex", "nofollow"});
        parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html", metadata);
        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        // no outlinks at all
        Assertions.assertEquals(0, statusTuples.size());
        Assertions.assertEquals(1, output.getEmitted().size());
        List<Object> parsedTuple = output.getEmitted().remove(0);
        // check in the metadata that the values match
        metadata = (Metadata) parsedTuple.get(2);
        Assertions.assertNotNull(metadata);
        boolean isNoIndex =
                Boolean.parseBoolean(metadata.getFirstValue(RobotsTags.ROBOTS_NO_INDEX));
        boolean isNoFollow =
                Boolean.parseBoolean(metadata.getFirstValue(RobotsTags.ROBOTS_NO_FOLLOW));
        boolean isNoCache =
                Boolean.parseBoolean(metadata.getFirstValue(RobotsTags.ROBOTS_NO_CACHE));
        Assertions.assertTrue(isNoIndex, "incorrect noIndex");
        Assertions.assertTrue(isNoFollow, "incorrect noFollow");
        Assertions.assertFalse(isNoCache, "incorrect noCache");
    }

    @Test
    void testRobotsMetaProcessor() throws IOException {
        bolt.prepare(
                new HashMap<>(), TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        for (int i = 0; i < tests.length; i++) {
            byte[] bytes = tests[i].getBytes(StandardCharsets.UTF_8);
            parse("https://stormcrawler.apache.org", bytes, new Metadata());
            Assertions.assertEquals(1, output.getEmitted().size());
            List<Object> parsedTuple = output.getEmitted().remove(0);
            // check in the metadata that the values match
            Metadata metadata = (Metadata) parsedTuple.get(2);
            Assertions.assertNotNull(metadata);
            boolean isNoIndex =
                    Boolean.parseBoolean(metadata.getFirstValue(RobotsTags.ROBOTS_NO_INDEX));
            boolean isNoFollow =
                    Boolean.parseBoolean(metadata.getFirstValue(RobotsTags.ROBOTS_NO_FOLLOW));
            boolean isNoCache =
                    Boolean.parseBoolean(metadata.getFirstValue(RobotsTags.ROBOTS_NO_CACHE));
            Assertions.assertEquals(
                    answers[i][0], isNoIndex, "incorrect noIndex value on doc " + i);
            Assertions.assertEquals(
                    answers[i][1], isNoFollow, "incorrect noFollow value on doc " + i);
            Assertions.assertEquals(
                    answers[i][2], isNoCache, "incorrect noCache value on doc " + i);
        }
    }

    @Test
    void testHTMLRedir() throws IOException {
        bolt.prepare(
                new HashMap<>(), TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        parse("http://www.somesite.com", "redir.html");
        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        // one for the redir + one for the discovered
        Assertions.assertEquals(2, statusTuples.size());
    }

    @Test
    void testExecuteWithOutlinksLimit() throws IOException {
        stormConf.put("parser.emitOutlinks.max.per.page", 5);
        bolt.prepare(stormConf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html");
        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        // outlinks being limited by property
        Assertions.assertEquals(5, statusTuples.size());
    }

    @Test
    void testExecuteWithOutlinksLimitDisabled() throws IOException {
        stormConf.put("parser.emitOutlinks.max.per.page", -1);
        bolt.prepare(stormConf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html");
        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        // outlinks NOT being limited by property, since is disabled with -1
        Assertions.assertEquals(25, statusTuples.size());
    }

    /**
     * text/plain content needs no markup parsing and should be passed on rather than treated as an
     * error, see issue #466.
     */
    @Test
    void testPlainText() throws IOException {
        bolt.prepare(
                new HashMap<>(), TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        String plain = "This is a plain text document.\nIt has no markup and no links.\n";
        Metadata metadata = new Metadata();
        metadata.setValue("Content-Type", "text/plain; charset=UTF-8");
        parse("http://www.example.com/page.txt", plain.getBytes(StandardCharsets.UTF_8), metadata);

        // not emitted on the status stream as an error
        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(0, statusTuples.size(), "plain text should not produce outlinks");

        // emitted on the default stream with the content as text
        Assertions.assertEquals(1, output.getEmitted().size());
        List<Object> parsedTuple = output.getEmitted().remove(0);
        String text = (String) parsedTuple.get(3);
        Assertions.assertEquals(plain, text, "text should be the verbatim plain text content");

        Metadata md = (Metadata) parsedTuple.get(2);
        Assertions.assertTrue(
                md.getFirstValue("parse.Content-Type").contains("text/plain"),
                "detected content-type should be text/plain");
        Assertions.assertEquals(JSoupParserBolt.class.getName(), md.getFirstValue("parsed.by"));
    }

    /**
     * The plain-text path does not run the TextExtractor, but it still honors {@code
     * textextractor.skip.after} by truncating the stored text, see issue #466.
     */
    @Test
    void testPlainTextTruncatedAtSkipAfter() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        conf.put("textextractor.skip.after", 10);
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        String plain = "0123456789ABCDEFGHIJ"; // 20 chars, cap is 10
        Metadata metadata = new Metadata();
        metadata.setValue("Content-Type", "text/plain; charset=UTF-8");
        parse("http://www.example.com/big.txt", plain.getBytes(StandardCharsets.UTF_8), metadata);

        Assertions.assertEquals(1, output.getEmitted().size());
        List<Object> parsedTuple = output.getEmitted().remove(0);
        String text = (String) parsedTuple.get(3);
        Assertions.assertEquals(
                "0123456789", text, "plain text should be truncated at textextractor.skip.after");
    }

    /**
     * The plain-text path honors {@code textextractor.no.text} and stores no text, see issue #466.
     */
    @Test
    void testPlainTextNoText() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        conf.put("textextractor.no.text", true);
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        String plain = "This is a plain text document.\n";
        Metadata metadata = new Metadata();
        metadata.setValue("Content-Type", "text/plain; charset=UTF-8");
        parse("http://www.example.com/page.txt", plain.getBytes(StandardCharsets.UTF_8), metadata);

        Assertions.assertEquals(1, output.getEmitted().size());
        List<Object> parsedTuple = output.getEmitted().remove(0);
        String text = (String) parsedTuple.get(3);
        Assertions.assertEquals("", text, "no text should be stored when textextractor.no.text");
    }

    @Test
    void testExecuteWithJavascriptLink() throws IOException {
        bolt.prepare(stormConf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        parse("http://www.javascriptlinks.com", "javascriptLinks.html");
        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(1, statusTuples.size());
        Assertions.assertEquals(Status.DISCOVERED, statusTuples.get(0).get(2));
        Assertions.assertEquals(
                "http://www.javascriptlinks.com/mylink", statusTuples.get(0).get(0));
    }
}
