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

package org.apache.stormcrawler.parse.filter;

import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class SubDocumentsParseFilter extends ParseFilter {

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {
        if (doc == null) {
            return;
        }

        NodeList links = findElements(doc, "a");
        for (int i = 0; i < links.getLength(); i++) {
            Node node = links.item(i);
            if (node instanceof Element) {
                String href = ((Element) node).getAttribute("href");
                if (href != null && !href.isEmpty()) {
                    parse.get(href);
                }
            }
        }
    }

    private NodeList findElements(Node node, String tagName) {
        if (node instanceof org.w3c.dom.Document) {
            return ((org.w3c.dom.Document) node).getElementsByTagName(tagName);
        }
        Node child = node.getFirstChild();
        while (child != null) {
            if (child instanceof Element) {
                return ((Element) child).getElementsByTagName(tagName);
            }
            child = child.getNextSibling();
        }
        return new EmptyNodeList();
    }

    @Override
    public boolean needsDOM() {
        return true;
    }

    private static class EmptyNodeList implements NodeList {
        @Override
        public Node item(int index) {
            return null;
        }

        @Override
        public int getLength() {
            return 0;
        }
    }
}
