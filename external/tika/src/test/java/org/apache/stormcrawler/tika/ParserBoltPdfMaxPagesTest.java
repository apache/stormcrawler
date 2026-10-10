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
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.TestUtil;
import org.apache.stormcrawler.parse.ParsingTester;
import org.apache.tika.config.JsonConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Checks that parser.tika.pdf.maxpages stops the parse of a PDF after the given number of pages.
 *
 * @see <a href="https://github.com/apache/stormcrawler/issues/1901">#1901</a>
 */
class ParserBoltPdfMaxPagesTest extends ParsingTester {

    private static final String URL = "https://example.org/doc.pdf";

    private static final String[] PAGES = {"alpha", "bravo", "charlie"};

    private static byte[] pdf;

    @BeforeAll
    static void createPdf() throws IOException {
        try (PDDocument doc = new PDDocument();
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            PDType1Font font = new PDType1Font(Standard14Fonts.FontName.HELVETICA);
            for (String word : PAGES) {
                PDPage page = new PDPage();
                doc.addPage(page);
                try (PDPageContentStream content = new PDPageContentStream(doc, page)) {
                    content.beginText();
                    content.setFont(font, 12);
                    content.newLineAtOffset(100, 700);
                    content.showText(word);
                    content.endText();
                }
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

    private void prepare(Map<String, Object> conf) {
        bolt.prepare(conf, TestUtil.getMockedTopologyContext(), new OutputCollector(output));
    }

    private void prepare(Integer maxPages) {
        Map<String, Object> conf = new HashMap<>();
        if (maxPages != null) {
            conf.put(ParserBolt.PDF_MAX_PAGES_PARAM, maxPages);
        }
        prepare(conf);
    }

    private String parsedText() throws IOException {
        parse(URL, pdf, new Metadata());
        Assertions.assertTrue(output.getEmitted(Constants.StatusStreamName).isEmpty());
        List<List<Object>> emitted = output.getEmitted();
        Assertions.assertEquals(1, emitted.size());
        return emitted.get(0).get(3).toString();
    }

    private static void assertPages(String text, int expected) {
        for (int i = 0; i < PAGES.length; i++) {
            Assertions.assertEquals(
                    i < expected, text.contains(PAGES[i]), PAGES[i] + " in " + text);
        }
    }

    @Test
    void stopsAfterMaxPages() throws IOException {
        prepare(1);
        assertPages(parsedText(), 1);
    }

    @Test
    void noLimitByDefault() throws IOException {
        prepare((Integer) null);
        assertPages(parsedText(), 3);
    }

    @Test
    void noLimitWithZero() throws IOException {
        prepare(0);
        assertPages(parsedText(), 3);
    }

    @Test
    void noLimitWithNegativeValue() throws IOException {
        prepare(-2);
        assertPages(parsedText(), 3);
    }

    @Test
    void keepsPdfSettingsOfTikaConfig() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("parser.tika.config.file", "test-pdf-tika-config.json");
        conf.put(ParserBolt.PDF_MAX_PAGES_PARAM, 2);
        prepare(conf);

        JsonConfig config = ((ParserBolt) bolt).createParseContext().getJsonConfig("pdf-parser");
        Assertions.assertNotNull(config);
        Assertions.assertTrue(config.json().contains("\"maxPages\":2"), config.json());
        Assertions.assertTrue(config.json().contains("\"extractInlineImages\":false"));
    }

    @Test
    void tikaConfigAloneLimitsPages() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        conf.put("parser.tika.config.file", "test-pdf-tika-config.json");
        prepare(conf);
        assertPages(parsedText(), 1);
    }

    @Test
    @Timeout(60)
    void stopsAfterMaxPagesInForkedJvm() throws IOException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(ParserBolt.PDF_MAX_PAGES_PARAM, 2);
        conf.put(ParserBolt.PARSE_TIMEOUT_PARAM, 10_000L);
        conf.put(ParserBolt.PIPES_JVM_ARGS_PARAM, "-Xmx256m");
        prepare(conf);
        try {
            assertPages(parsedText(), 2);
        } finally {
            bolt.cleanup();
        }
    }
}
