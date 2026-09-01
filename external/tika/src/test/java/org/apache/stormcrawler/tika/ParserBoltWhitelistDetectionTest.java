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
import org.apache.http.HttpHeaders;
import org.apache.storm.task.OutputCollector;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestUtil;
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
     * The whitelist allows Word documents (application/.+word.*). The server header claims
     * Word, but the body bytes are plain HTML. After the fix, detection on bytes yields
     * text/html which does NOT match the whitelist, so the document must be rejected with ERROR.
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

        System.out.println("detected type: " + metadata.getFirstValue("parse.Content-Type"));
        System.out.println("emitted documents: " + output.getEmitted().size());

        List<List<Object>> status = output.getEmitted(Constants.StatusStreamName);
        Assertions.assertEquals(
                1, status.size(), "content not matching the whitelist should be rejected");
        Assertions.assertEquals(
                Status.ERROR,
                status.get(0).get(2),
                "status should be ERROR for mismatched content type");
    }

    /**
     * Sanity check: when parse.Content-Type IS already present (e.g. set by JSoupParserBolt),
     * the whitelist check must still use it directly and not re-detect.
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
        metadata.addValue("http." + HttpHeaders.CONTENT_TYPE, "text/html; charset=UTF-8");

        byte[] content =
                "<html><body><p>hello</p></body></html>".getBytes(StandardCharsets.UTF_8);
        parse("https://example.org/index.html", content, metadata);

        // document should pass the whitelist and be emitted (no ERROR on status stream)
        List<List<Object>> status = output.getEmitted(Constants.StatusStreamName);
        boolean hasError =
                status != null
                        && status.stream()
                                .anyMatch(row -> Status.ERROR.equals(row.get(2)));
        Assertions.assertFalse(hasError, "whitelisted HTML document should not be rejected");
    }
}
