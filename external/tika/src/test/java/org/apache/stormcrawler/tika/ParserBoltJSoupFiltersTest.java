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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.apache.storm.task.OutputCollector;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.parse.ParsingTester;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Checks that the ParserBolt applies the JSoupFilters to the documents parsed by Tika. */
class ParserBoltJSoupFiltersTest extends ParsingTester {

    private static byte[] pdf;

    @BeforeAll
    static void createPdf() throws IOException {
        try (PDDocument doc = new PDDocument();
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            PDPage page = new PDPage();
            doc.addPage(page);
            try (PDPageContentStream content = new PDPageContentStream(doc, page)) {
                content.beginText();
                content.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 12);
                content.newLineAtOffset(100, 700);
                content.showText("alpha");
                content.endText();
            }
            doc.save(out);
            pdf = out.toByteArray();
        }
    }

    @BeforeEach
    void setupParserBolt() {
        bolt = new ParserBolt();
        setupParserBolt(bolt);
    }

    private void prepare(String jsoupConfig, Map<String, Object> conf) {
        conf.put("jsoup.filters.config.file", jsoupConfig);
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    private Metadata emittedMetadata() {
        List<List<Object>> emitted = output.getEmitted();
        Assertions.assertEquals(1, emitted.size());
        return (Metadata) emitted.get(0).get(2);
    }

    private void checkHtml(Map<String, Object> conf) throws IOException {
        // same expressions as the JSoupParserBolt tests in core; the meta elements are not
        // checked as Tika rewrites the head from its own metadata (e.g. dc:subject)
        prepare("test.xpath.jsoupfilters.json", conf);
        try {
            parse("https://example.com", "xpath-test.html");
        } finally {
            bolt.cleanup();
        }
        Metadata metadata = emittedMetadata();
        Assertions.assertEquals("XPath Test Page", metadata.getFirstValue("title_text"));
        Assertions.assertEquals(
                "Hello world, this is a test.", metadata.getFirstValue("tidy_text").strip());
        Assertions.assertTrue(metadata.getFirstValue("html_content").contains("<strong>world"));
        // span elements and class attributes are dropped by Tika's DefaultHtmlMapper
        Assertions.assertEquals(2, metadata.getValues("concept_upper").length);
        Assertions.assertTrue(metadata.getFirstValue("fallback").contains("Main Title"));
    }

    @Test
    void testHtml() throws IOException {
        checkHtml(new HashMap<>());
    }

    /** The IdentityHtmlMapper used by default must be applied in the forked JVM too. */
    @Test
    @Timeout(60)
    void testHtmlWithPipes() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(ParserBolt.PARSE_TIMEOUT_PARAM, 30_000L);
        conf.put(ParserBolt.PIPES_JVM_ARGS_PARAM, "-Xmx256m");
        checkHtml(conf);
    }

    @Test
    void testPdf() throws IOException {
        prepare("test.pdf.jsoupfilters.json", new HashMap<>());
        parse("https://example.org/doc.pdf", pdf, new Metadata());
        Assertions.assertEquals("alpha", emittedMetadata().getFirstValue("page_text").strip());
    }

    @Test
    @Timeout(60)
    void testPdfWithPipes() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(ParserBolt.PARSE_TIMEOUT_PARAM, 30_000L);
        conf.put(ParserBolt.PIPES_JVM_ARGS_PARAM, "-Xmx256m");
        prepare("test.pdf.jsoupfilters.json", conf);
        try {
            parse("https://example.org/doc.pdf", pdf, new Metadata());
            Assertions.assertEquals("alpha", emittedMetadata().getFirstValue("page_text").strip());
        } finally {
            bolt.cleanup();
        }
    }

    @Test
    void testNoJSoupFilters() throws IOException {
        bolt.prepare(
                new HashMap<>(), TestUtil.getMockedTopologyContext(), new OutputCollector(output));
        parse("https://example.org/doc.pdf", pdf, new Metadata());
        Assertions.assertNull(emittedMetadata().getFirstValue("page_text"));
    }
}
