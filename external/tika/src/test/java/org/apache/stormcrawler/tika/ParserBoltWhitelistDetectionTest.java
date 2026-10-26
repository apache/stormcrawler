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
import org.apache.stormcrawler.internal.httpcomponents.HttpHeaders;
import org.apache.stormcrawler.parse.ParsingTester;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test for: when no parse.Content-Type is present, ParserBolt must evaluate
 * parser.mimetype.whitelist against the byte-detected MIME type (via tika.detect()), not the
 * server-declared HTTP Content-Type header. Previously the whitelist checked the header while
 * Tika's AutoDetectParser dispatched on the bytes, allowing a server to claim a whitelisted type
 * while serving arbitrary content.
 */
class ParserBoltWhitelistDetectionTest extends ParsingTester {

    @BeforeEach
    void setupParserBolt() {
        bolt = new ParserBolt();
        setupParserBolt(bolt);
    }

    /**
     * The whitelist allows Word documents (application/.+word.*). The server header claims Word,
     * but the body bytes are plain HTML. After the fix, detection on bytes yields text/html which
     * does NOT match the whitelist, so the document must be rejected with ERROR.
     */
    @Test
    void whitelistAppliesToTheDetectedType() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        // the whitelist shipped by the archetypes
        conf.put("parser.mimetype.whitelist", "application/.+word.*");
        conf.put(ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "http.");
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        // no parse.Content-Type: no JSoupParserBolt upstream, or detect.mimetype disabled
        Metadata metadata = new Metadata();
        metadata.addValue(
                "http." + HttpHeaders.CONTENT_TYPE,
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document");

        // the body is NOT a word document
        byte[] content =
                "<html><body><p>not a word document</p></body></html>"
                        .getBytes(StandardCharsets.UTF_8);
        parse("https://example.org/doc.docx", content, metadata);

        // detection must have resolved to text/html, not the server-declared Word type
        String detected = metadata.getFirstValue("parse.Content-Type");
        Assertions.assertNotNull(detected, "detected MIME type must be written back");
        Assertions.assertTrue(
                detected.startsWith("text/html"),
                "HTML bytes must be detected as text/html, got: " + detected);

        List<List<Object>> status = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(
                1, status.size(), "content not matching the whitelist should be rejected");
        Assertions.assertEquals(
                Status.ERROR,
                status.get(0).get(2),
                "status should be ERROR for mismatched content type");
    }

    /**
     * Sanity check: when parse.Content-Type IS already present the whitelist must use it directly
     * without re-detecting. Uses plain-text bytes at a .txt URL so that any re-detection would
     * yield text/plain, not text/html — proving the preset value is trusted.
     */
    @Test
    void whitelistUsesPreexistingParsedContentType() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        conf.put("parser.mimetype.whitelist", "text/html.*");
        conf.put(ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "http.");
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        Metadata metadata = new Metadata();
        // simulate JSoupParserBolt having detected the type already
        metadata.addValue("parse.Content-Type", "text/html; charset=UTF-8");
        // HTTP header also says text/plain to make re-detection diverge if it runs
        metadata.addValue("http." + HttpHeaders.CONTENT_TYPE, "text/plain");

        // plain-text bytes at a .txt URL: re-detection would yield text/plain, not text/html
        byte[] content = "just some plain text, no html tags".getBytes(StandardCharsets.UTF_8);
        parse("https://example.org/file.txt", content, metadata);

        // proof that the pre-existing parse.Content-Type was trusted: if the whitelist had
        // re-detected the bytes (yielding text/plain), the text/html.* pattern would not have
        // matched and the bolt would have emitted ERROR — so no ERROR means the preset was used.
        // (note: AutoDetectParser overwrites parse.Content-Type on a successful parse, so the
        // value after parse() reflects parse-time detection, not the whitelist check.)
        List<List<Object>> status = output.getEmitted(Constants.StatusStreamName);
        boolean hasError =
                status != null && status.stream().anyMatch(row -> Status.ERROR.equals(row.get(2)));
        Assertions.assertFalse(hasError, "whitelisted document should not be rejected");
    }

    /**
     * Plain-text bytes with a .html URL extension. Without the filename hint, Tika resolves the
     * ambiguous bytes as text/plain; with it, the extension pushes detection to text/html. The
     * whitelist is set to text/html.*, so the document must be accepted — verifying that the same
     * RESOURCE_NAME_KEY hint is passed to both the whitelist check and the parser dispatch.
     */
    @Test
    void filenameHintInfluencesDetection() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        conf.put("parser.mimetype.whitelist", "text/html.*");
        conf.put(ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "http.");
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));

        // no parse.Content-Type, no Content-Type header — detection relies on bytes + filename
        Metadata metadata = new Metadata();

        // plain text bytes: no HTML magic, ambiguous without the filename hint
        byte[] content = "just some plain text, no html tags".getBytes(StandardCharsets.UTF_8);

        // .html extension should push detection to text/html
        parse("https://example.org/page.html", content, metadata);

        // the filename hint must have steered detection to text/html
        String detected = metadata.getFirstValue("parse.Content-Type");
        Assertions.assertNotNull(detected, "detected MIME type must be written back");
        Assertions.assertTrue(
                detected.startsWith("text/html"),
                ".html filename hint must resolve detection to text/html, got: " + detected);

        List<List<Object>> status = output.getEmitted(Constants.StatusStreamName);
        boolean hasError =
                status != null && status.stream().anyMatch(row -> Status.ERROR.equals(row.get(2)));
        Assertions.assertFalse(
                hasError, "document with .html URL should be accepted by text/html.* whitelist");
    }
}
