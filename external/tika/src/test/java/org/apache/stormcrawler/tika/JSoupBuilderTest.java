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

import java.io.StringReader;
import javax.xml.parsers.SAXParserFactory;
import org.jsoup.nodes.DataNode;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

class JSoupBuilderTest {

    private static Document build(String xhtml) throws Exception {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(true);
        XMLReader reader = factory.newSAXParser().getXMLReader();
        JSoupBuilder builder = new JSoupBuilder("https://example.com/dir/page.html");
        reader.setContentHandler(builder);
        reader.parse(new InputSource(new StringReader(xhtml)));
        return builder.getDocument();
    }

    @Test
    void testStructure() throws Exception {
        Document doc =
                build(
                        "<html xmlns=\"http://www.w3.org/1999/xhtml\"><head><title>T</title>"
                                + "<script>if (a &lt; b) {}</script></head>"
                                + "<body><P CLASS=\"x\">one &amp; <b>two</b> three</P>"
                                + "<a href=\"other.html\">link</a></body></html>");

        Assertions.assertEquals("T", doc.title());
        Assertions.assertNotNull(doc.body());

        Element p = doc.selectFirst("p.x");
        Assertions.assertNotNull(p);
        Assertions.assertEquals("one & two three", p.text());
        // text split by the parser into several events ends up in a single node
        Assertions.assertEquals(3, p.childNodeSize());

        Element script = doc.selectFirst("script");
        Assertions.assertInstanceOf(DataNode.class, script.childNode(0));
        Assertions.assertEquals("if (a < b) {}", script.data());

        Element a = doc.selectFirst("a");
        Assertions.assertEquals("https://example.com/dir/other.html", a.absUrl("href"));
        Assertions.assertFalse(doc.selectFirst("html").hasAttr("xmlns"));
    }

    @Test
    void testTruncatedInput() throws Exception {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(true);
        XMLReader reader = factory.newSAXParser().getXMLReader();
        JSoupBuilder builder = new JSoupBuilder("https://example.com/");
        reader.setContentHandler(builder);
        Assertions.assertThrows(
                Exception.class,
                () ->
                        reader.parse(
                                new InputSource(
                                        new StringReader("<html><body><p>kept</p><p>cut"))));
        Assertions.assertEquals("kept", builder.getDocument().selectFirst("p").text());
    }
}
