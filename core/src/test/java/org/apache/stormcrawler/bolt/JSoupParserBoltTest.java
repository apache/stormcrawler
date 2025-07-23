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

    @Test
    /** Checks that content in script is not included in the text representation */
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

    @Test
    /** Checks that individual links marked as rel="nofollow" are not followed */
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
