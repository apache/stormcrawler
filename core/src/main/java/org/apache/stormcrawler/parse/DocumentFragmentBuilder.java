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

package org.apache.stormcrawler.parse;

import org.apache.html.dom.HTMLDocumentImpl;
import org.jetbrains.annotations.NotNull;
import org.jsoup.nodes.Attribute;
import org.jsoup.select.NodeTraversor;
import org.jsoup.select.NodeVisitor;
import org.w3c.dom.Comment;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Element;
import org.w3c.dom.Text;

/** Adapted from org.jsoup.helper.W3CDom but does not transfer namespaces. * */
public final class DocumentFragmentBuilder {

    /** Restrict instantiation. */
    private DocumentFragmentBuilder() {}

    public static DocumentFragment fromJsoup(org.jsoup.nodes.Document jsoupDocument) {
        HTMLDocumentImpl htmlDoc = new HTMLDocumentImpl();
        htmlDoc.setErrorChecking(false);
        DocumentFragment fragment = htmlDoc.createDocumentFragment();
        org.jsoup.nodes.Element rootEl = jsoupDocument.child(0); // skip the
        // #root node
        NodeTraversor.traverse(new W3CBuilder(htmlDoc, fragment), rootEl);
        return fragment;
    }

    /** Implements the conversion by walking the input. */
    protected static class W3CBuilder implements NodeVisitor {
        private final HTMLDocumentImpl doc;
        private final DocumentFragment fragment;

        private Element dest;

        public W3CBuilder(HTMLDocumentImpl doc, DocumentFragment fragment) {
            this.fragment = fragment;
            this.doc = doc;
        }

        public void head(@NotNull org.jsoup.nodes.Node source, int depth) {
            if (source instanceof org.jsoup.nodes.Element) {
                org.jsoup.nodes.Element sourceEl = (org.jsoup.nodes.Element) source;
                Element el = doc.createElement(sourceEl.tagName());
                copyAttributes(sourceEl, el);
                if (dest == null) { // sets up the root
                    fragment.appendChild(el);
                } else {
                    dest.appendChild(el);
                }
                dest = el; // descend
            } else if (source instanceof org.jsoup.nodes.TextNode) {
                org.jsoup.nodes.TextNode sourceText = (org.jsoup.nodes.TextNode) source;
                Text text = doc.createTextNode(sourceText.getWholeText());
                dest.appendChild(text);
            } else if (source instanceof org.jsoup.nodes.Comment) {
                org.jsoup.nodes.Comment sourceComment = (org.jsoup.nodes.Comment) source;
                Comment comment = doc.createComment(sourceComment.getData());
                dest.appendChild(comment);
            } else if (source instanceof org.jsoup.nodes.DataNode) {
                org.jsoup.nodes.DataNode sourceData = (org.jsoup.nodes.DataNode) source;
                Text node = doc.createTextNode(sourceData.getWholeData());
                dest.appendChild(node);
            } else {
                // unhandled
            }
        }

        public void tail(@NotNull org.jsoup.nodes.Node source, int depth) {
            if (source instanceof org.jsoup.nodes.Element
                    && dest.getParentNode() instanceof Element) {
                dest = (Element) dest.getParentNode(); // undescend. cromulent.
            }
        }

        private void copyAttributes(org.jsoup.nodes.Node source, Element el) {
            for (Attribute attribute : source.attributes()) {
                // valid xml attribute names are: ^[a-zA-Z_:][-a-zA-Z0-9_:.]
                String key = attribute.getKey().replaceAll("[^-a-zA-Z0-9_:.]", "");
                if (key.matches("[a-zA-Z_:][-a-zA-Z0-9_:.]*")) {
                    el.setAttribute(key, attribute.getValue());
                }
            }
        }
    }
}
