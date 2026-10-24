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
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.SAXParserFactory;
import org.apache.tika.sax.XHTMLContentHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

class NamespaceRestoringHandlerTest {

    /** Records the elements and attributes received, as uri|localName|qualifiedName. */
    private static final class Recorder extends DefaultHandler {
        final List<String> events = new ArrayList<>();

        @Override
        public void startPrefixMapping(String prefix, String uri) {
            events.add("prefix " + prefix + "=" + uri);
        }

        @Override
        public void endPrefixMapping(String prefix) {
            events.add("/prefix " + prefix);
        }

        @Override
        public void startElement(
                String uri, String localName, String qualifiedName, Attributes atts) {
            events.add(uri + "|" + localName + "|" + qualifiedName);
            for (int i = 0; i < atts.getLength(); i++) {
                events.add(
                        "@"
                                + atts.getURI(i)
                                + "|"
                                + atts.getLocalName(i)
                                + "|"
                                + atts.getQName(i)
                                + "="
                                + atts.getValue(i));
            }
        }

        @Override
        public void endElement(String uri, String localName, String qualifiedName) {
            events.add("/" + uri + "|" + localName + "|" + qualifiedName);
        }
    }

    private static List<String> parse(String xml) throws Exception {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(false);
        XMLReader reader = factory.newSAXParser().getXMLReader();
        Recorder recorder = new Recorder();
        reader.setContentHandler(new NamespaceRestoringHandler(recorder));
        reader.parse(new InputSource(new StringReader(xml)));
        return recorder.events;
    }

    @Test
    void testUndeclaredPrefixes() throws Exception {
        String xhtml = XHTMLContentHandler.XHTML;
        List<String> events =
                parse(
                        "<html xmlns=\""
                                + xhtml
                                + "\"><body><use xlink:href=\"#i\" x-on:click=\"go\"/>"
                                + "<o:p xml:lang=\"en\"/></body></html>");
        Assertions.assertEquals(
                List.of(
                        "prefix =" + xhtml,
                        xhtml + "|html|html",
                        xhtml + "|body|body",
                        xhtml + "|use|use",
                        "@|xlink:href|xlink:href=#i",
                        "@|x-on:click|x-on:click=go",
                        "/" + xhtml + "|use|use",
                        xhtml + "|o:p|o:p",
                        "@http://www.w3.org/XML/1998/namespace|lang|xml:lang=en",
                        "/" + xhtml + "|o:p|o:p",
                        "/" + xhtml + "|body|body",
                        "/" + xhtml + "|html|html",
                        "/prefix "),
                events);
    }

    @Test
    void testDeclaredPrefix() throws Exception {
        List<String> events = parse("<a xmlns:s=\"urn:s\"><s:b s:c=\"1\"/></a>");
        Assertions.assertEquals(
                List.of(
                        "prefix s=urn:s",
                        "|a|a",
                        "urn:s|b|s:b",
                        "@urn:s|c|s:c=1",
                        "/urn:s|b|s:b",
                        "/|a|a",
                        "/prefix s"),
                events);
    }
}
