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

import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.DataNode;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.TextNode;
import org.jsoup.parser.ParseSettings;
import org.jsoup.parser.Tag;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Builds a JSoup document from the XHTML SAX events produced by Tika, so that JSoupFilters can be
 * applied to any format Tika can parse. Element names are lowercased, namespaces are dropped. The
 * document holds whatever was received so far if the parse stops early, e.g. when the text limit is
 * reached.
 */
public class JSoupBuilder extends DefaultHandler {

    private final Document document;

    private Element current;

    private final StringBuilder text = new StringBuilder();

    /**
     * @param baseUri URL of the document, used to resolve relative links
     */
    public JSoupBuilder(String baseUri) {
        document = new Document(baseUri);
        current = document;
    }

    public Document getDocument() {
        flushText();
        return document;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) {
        flushText();
        String name = StringUtils.isNotEmpty(localName) ? localName : qName;
        Element element =
                new Element(Tag.valueOf(name, ParseSettings.htmlDefault), document.baseUri());
        for (int i = 0; i < atts.getLength(); i++) {
            String attName = atts.getLocalName(i);
            if (StringUtils.isEmpty(attName)) {
                attName = atts.getQName(i);
            }
            if (attName.startsWith("xmlns")) {
                continue;
            }
            element.attr(attName, atts.getValue(i));
        }
        current.appendChild(element);
        current = element;
    }

    @Override
    public void endElement(String uri, String localName, String qName) {
        flushText();
        if (current != document) {
            current = current.parent();
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) {
        text.append(ch, start, length);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) {
        text.append(ch, start, length);
    }

    /** Adds the text received since the last element event as a single node. */
    private void flushText() {
        if (text.length() == 0) {
            return;
        }
        String value = text.toString();
        text.setLength(0);
        // nothing outside of the root element
        if (current == document) {
            return;
        }
        // keep the content of scripts and styles as is, like the JSoup parser does
        String name = current.normalName();
        if ("script".equals(name) || "style".equals(name)) {
            current.appendChild(new DataNode(value));
        } else {
            current.appendChild(new TextNode(value));
        }
    }
}
