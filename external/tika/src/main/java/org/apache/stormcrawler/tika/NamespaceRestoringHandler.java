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

import java.util.Enumeration;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.NamespaceSupport;

/**
 * Resolves the namespaces of the events produced by a SAX parser which is not namespace aware, so
 * that the XML returned by a Tika Pipes fork reaches the handlers as an in-process parse would. The
 * fork writes the names kept by the IdentityHtmlMapper as they are, e.g. {@code xlink:href} or
 * {@code x-on:click}, without declaring their prefix, which a namespace aware parser rejects. A
 * name whose prefix is not declared is kept whole as the local name, an element without a declared
 * prefix gets the default namespace in scope.
 */
class NamespaceRestoringHandler extends ContentHandlerDecorator {

    private static final String XMLNS = "xmlns";

    private final NamespaceSupport namespaces = new NamespaceSupport();

    private final String[] parts = new String[3];

    NamespaceRestoringHandler(ContentHandler handler) {
        super(handler);
    }

    @Override
    public void startElement(String uri, String localName, String qualifiedName, Attributes atts)
            throws SAXException {
        namespaces.pushContext();
        for (int i = 0; i < atts.getLength(); i++) {
            String prefix = declaredPrefix(atts.getQName(i));
            if (prefix != null) {
                namespaces.declarePrefix(prefix, atts.getValue(i));
                super.startPrefixMapping(prefix, atts.getValue(i));
            }
        }

        AttributesImpl resolved = new AttributesImpl();
        for (int i = 0; i < atts.getLength(); i++) {
            String name = atts.getQName(i);
            if (declaredPrefix(name) != null) {
                continue;
            }
            if (namespaces.processName(name, parts, true) != null) {
                resolved.addAttribute(parts[0], parts[1], name, atts.getType(i), atts.getValue(i));
            } else {
                resolved.addAttribute("", name, name, atts.getType(i), atts.getValue(i));
            }
        }

        resolveElement(qualifiedName);
        super.startElement(parts[0], parts[1], qualifiedName, resolved);
    }

    @Override
    public void endElement(String uri, String localName, String qualifiedName) throws SAXException {
        resolveElement(qualifiedName);
        super.endElement(parts[0], parts[1], qualifiedName);
        Enumeration<?> declared = namespaces.getDeclaredPrefixes();
        while (declared.hasMoreElements()) {
            super.endPrefixMapping((String) declared.nextElement());
        }
        namespaces.popContext();
    }

    /** Sets the namespace and local name of an element in {@link #parts}. */
    private void resolveElement(String qualifiedName) {
        if (namespaces.processName(qualifiedName, parts, false) == null) {
            parts[0] = StringUtils.defaultString(namespaces.getURI(""));
            parts[1] = qualifiedName;
        }
    }

    /** Returns the prefix declared by an attribute, "" for the default namespace, or null. */
    private static String declaredPrefix(String attributeName) {
        if (attributeName.equals(XMLNS)) {
            return "";
        }
        if (attributeName.startsWith(XMLNS + ":")) {
            return attributeName.substring(XMLNS.length() + 1);
        }
        return null;
    }
}
