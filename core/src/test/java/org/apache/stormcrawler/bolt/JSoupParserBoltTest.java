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
import org.apache.stormcrawler.internal.httpcomponents.HttpHeaders;
import org.apache.stormcrawler.parse.ParsingTester;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.CharsetIdentification;
import org.apache.stormcrawler.util.RobotsTags;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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

    /** A failure inside charset detection is a parse error of that URL, not a dead worker. */
    @Test
    void charsetDetectionFailureIsReportedAsParseError() throws IOException {
        bolt.prepare(
                new HashMap<>(), TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        try (MockedStatic<CharsetIdentification> detection =
                Mockito.mockStatic(CharsetIdentification.class)) {
            detection
                    .when(
                            () ->
                                    CharsetIdentification.getCharset(
                                            Mockito.any(),
                                            Mockito.any(),
                                            Mockito.anyInt(),
                                            Mockito.any()))
                    .thenThrow(new StackOverflowError());
            parse("https://stormcrawler.apache.org", "stormcrawler.apache.org.html");
        }
        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(1, statusTuples.size());
        Assertions.assertEquals(Status.ERROR, statusTuples.get(0).get(2));
        Metadata metadata = (Metadata) statusTuples.get(0).get(1);
        Assertions.assertEquals(
                "content parsing", metadata.getFirstValue(Constants.STATUS_ERROR_SOURCE));
        Assertions.assertEquals(1, output.getAckedTuples().size());
        Assertions.assertTrue(output.getEmitted().isEmpty(), "no document must be emitted");
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

    /**
     * The target of a meta refresh is discovered with SitemapFilter in the chain even if the site
     * declares a sitemap: the filter is meant for outlinks of non-sitemap pages.
     */
    @Test
    void testHTMLRedirWithSitemapFilter() throws IOException {
        stormConf.put("sitemap.discovery", true);
        stormConf.put("urlfilters.config.file", "urlfilters-sitemap-only.json");
        bolt.prepare(stormConf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        Metadata metadata = new Metadata();
        metadata.setValue(SiteMapParserBolt.foundSitemapKey, "true");
        parse("http://www.somesite.com", "redir.html", metadata);
        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(2, statusTuples.size());
        Assertions.assertTrue(
                statusTuples.stream()
                        .anyMatch(
                                t ->
                                        t.get(0).equals("http://www.somesite.com/site")
                                                && t.get(2) == Status.DISCOVERED));
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

    /**
     * Relative outlinks must be resolved against the document's {@code <base href>} element when
     * one is present, matching browser behaviour, rather than against the document URL. Otherwise a
     * page served from a sub-path with a base pointing at the domain root doubles the path segment.
     */
    @Test
    void testBaseHrefOutlinkResolution() throws IOException {
        bolt.prepare(stormConf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        String html =
                "<html><head>"
                        + "<base href=\"https://www.example.be/\">"
                        + "</head><body>"
                        + "<a href=\"fr/rayons/stockage-archivage-de-donnees/218725\">Stockage</a>"
                        + "</body></html>";

        Metadata metadata = new Metadata();
        metadata.setValue("Content-Type", "text/html; charset=UTF-8");
        parse(
                "https://www.example.be/fr/rayons/actualites-reportages/216943",
                html.getBytes(StandardCharsets.UTF_8),
                metadata);

        List<List<Object>> statusTuples = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(1, statusTuples.size());
        Assertions.assertEquals(Status.DISCOVERED, statusTuples.get(0).get(2));
        // resolved against the <base href> (domain root), NOT against the document path,
        // which would have produced .../actualites-reportages/fr/rayons/stockage-...
        Assertions.assertEquals(
                "https://www.example.be/fr/rayons/stockage-archivage-de-donnees/218725",
                statusTuples.get(0).get(0));
    }

    /**
     * The fetcher stores the response headers under protocol.md.prefix, so the charset declared in
     * the Content-Type has to be read with that prefix. Without it the header is never seen, the
     * header and the meta charset can never agree, and ICU guesses a UTF-8 page whose detection
     * window is plain ASCII as ISO-8859-1, see issue #2250.
     */
    @ParameterizedTest(name = "detect.charset.fast={0}")
    @ValueSource(booleans = {false, true})
    void prefixedContentTypeCharsetIsUsed(boolean fast) throws IOException {
        Map<String, Object> parserConfig = new HashMap<>();
        parserConfig.put("protocol.md.prefix", "protocol.");
        parserConfig.put("detect.charset.maxlength", 2048);
        parserConfig.put("detect.charset.fast", fast);
        bolt.prepare(
                parserConfig, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        // an ASCII head longer than the detection window, like most real pages. The default
        // detection uses the header when it agrees with the meta charset; the fast one returns the
        // header first, so it is tested without a meta charset, which it would use otherwise
        String meta = fast ? "" : "<meta charset=\"utf-8\">";
        String head =
                meta
                        + "<meta name=\"description\" content=\""
                        + "free shipping on every order ".repeat(100)
                        + "\">";
        String text = "What’s new: café imotine™";
        byte[] content =
                ("<html><head>" + head + "</head><body><p>" + text + "</p></body></html>")
                        .getBytes(StandardCharsets.UTF_8);
        Metadata metadata = new Metadata();
        metadata.setValue("protocol." + HttpHeaders.CONTENT_TYPE, "text/html; charset=UTF-8");
        parse("http://www.example.com/page.html", content, metadata);

        Assertions.assertEquals(1, output.getEmitted().size());
        String parsedText = (String) output.getEmitted().get(0).get(3);
        Assertions.assertTrue(parsedText.contains(text), parsedText);
    }
}
