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

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.stormcrawler.util.ConfUtils;
import org.jetbrains.annotations.Contract;
import org.jsoup.helper.Validate;
import org.jsoup.internal.StringUtil;
import org.jsoup.nodes.CDataNode;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.jsoup.select.NodeVisitor;

/**
 * Filters the text extracted from HTML documents, used by JSoupParserBolt. Configured with optional
 * inclusion patterns based on <a href="https://jsoup.org/cookbook/extracting-data/selector-syntax">
 * JSoup selectors</a>, as well as a list of tags to be excluded.
 *
 * <p>Replaces ContentFilter.
 *
 * <p>The first matching inclusion pattern is used or the whole document if no expressions are
 * configured or no match has been found.
 *
 * <p>The TextExtraction can be configured as so:
 *
 * <pre>{@code
 * textextractor.include.pattern:
 *  - DIV[id="maincontent"]
 *  - DIV[itemprop="articleBody"]
 *  - ARTICLE
 *
 * textextractor.exclude.tags:
 *  - STYLE
 *  - SCRIPT
 *
 * }</pre>
 *
 * @since 1.13
 */
public class JSoupTextExtractor implements TextExtractor {

    private final List<String> inclusionPatterns;
    private final Set<String> excludedTags;
    private final boolean noText;
    private final int maxTextSize;

    public JSoupTextExtractor(Map<String, Object> stormConf) {
        maxTextSize = ConfUtils.getInt(stormConf, TEXT_MAX_TEXT_PARAM_NAME, -1);
        noText = ConfUtils.getBoolean(stormConf, NO_TEXT_PARAM_NAME, false);
        inclusionPatterns = ConfUtils.loadListFromConf(INCLUDE_PARAM_NAME, stormConf);
        excludedTags = new HashSet<>();
        ConfUtils.loadListFromConf(EXCLUDE_PARAM_NAME, stormConf)
                .forEach((s) -> excludedTags.add(s.toLowerCase(Locale.ROOT)));
    }

    public String text(Object o) {
        // not interested in getting any text?
        if (noText) {
            return "";
        }

        if (o instanceof Element element) {

            final StringBuilder accum = new StringBuilder();

            // no patterns at all - return the text from the whole document
            if (inclusionPatterns.size() == 0 && excludedTags.size() == 0) {
                text(element, accum);
            } else {
                Elements matches = new Elements();

                for (String pattern : inclusionPatterns) {
                    matches = element.select(pattern);
                    if (!matches.isEmpty()) {
                        break;
                    }
                }

                // if nothing matches or no patterns were defined use the whole doc
                if (matches.isEmpty()) {
                    matches.add(element);
                }

                for (Element node : matches) {
                    text(node, accum);
                    accum.append("\n");
                }
            }

            return accum.toString().trim();
        }
        return "";
    }

    private void text(Node node, final StringBuilder accum) {
        traverse(
                new NodeVisitor() {

                    private Node excluded = null;

                    public void head(Node node, int depth) {
                        if (excluded == null && node instanceof TextNode) {
                            TextNode textNode = (TextNode) node;
                            appendNormalisedText(accum, textNode);
                        } else if (node instanceof Element) {
                            Element element = (Element) node;
                            if (excludedTags.contains(element.tagName())) {
                                excluded = element;
                            }
                            if (accum.length() > 0
                                    && (element.isBlock() || element.tag().getName().equals("br"))
                                    && !lastCharIsWhitespace(accum)) {
                                accum.append(' ');
                            }
                        }
                    }

                    public void tail(Node node, int depth) {
                        // make sure there is a space between block tags and immediately
                        // following text nodes <div>One</div>Two should be "One Two".
                        if (node instanceof Element) {
                            Element element = (Element) node;
                            if (element == excluded) {
                                excluded = null;
                            }
                            if (element.isBlock()
                                    && (node.nextSibling() instanceof TextNode)
                                    && !lastCharIsWhitespace(accum)) {
                                accum.append(' ');
                            }
                        }
                    }
                },
                node,
                maxTextSize,
                accum);
    }

    /**
     * Start a depth-first traverse of the root and all of its descendants.
     *
     * @param visitor Node visitor.
     * @param root the root node point to traverse.
     */
    private void traverse(NodeVisitor visitor, Node root, int maxSize, StringBuilder builder) {
        Validate.notNull(visitor, "null visitor in traverse method");
        Validate.notNull(root, "null root node in traverse method");
        Node node = root;
        int depth = 0;

        while (node != null) {
            // interrupts if too much text has already been produced
            if (maxSize > 0 && builder.length() >= maxSize) {
                return;
            }

            Node parent =
                    node.parentNode(); // remember parent to find nodes that get replaced in .head
            int origSize = parent != null ? parent.childNodeSize() : 0;
            Node next = node.nextSibling();

            visitor.head(node, depth); // visit current node
            if (parent != null && !node.hasParent()) { // removed or replaced
                if (origSize == parent.childNodeSize()) { // replaced
                    node =
                            parent.childNode(
                                    node.siblingIndex()); // replace ditches parent but keeps
                    // sibling index
                } else { // removed
                    node = next;
                    if (node == null) { // last one, go up
                        node = parent;
                        depth--;
                    }
                    continue; // don't tail removed
                }
            }

            if (node.childNodeSize() > 0) { // descend
                node = node.childNode(0);
                depth++;
            } else {
                // when no more siblings, ascend
                while (node.nextSibling() == null && depth > 0) {
                    visitor.tail(node, depth);
                    node = node.parentNode();
                    depth--;
                }
                visitor.tail(node, depth);
                if (node == root) {
                    break;
                }
                node = node.nextSibling();
            }
        }
    }

    private static void appendNormalisedText(final StringBuilder accum, final TextNode textNode) {
        final String text = textNode.getWholeText();
        if (textNode instanceof CDataNode || preserveWhitespace(textNode.parent())) {
            accum.append(text);
        } else {
            StringUtil.appendNormalisedWhitespace(accum, text, lastCharIsWhitespace(accum));
        }
    }

    @Contract("null -> false")
    static boolean preserveWhitespace(Node node) {
        if (node == null) {
            return false;
        }
        // looks only at this element and five levels up, to prevent recursion &
        // needless stack searches
        if (node instanceof Element el) {
            int i = 0;
            do {
                if (el.tag().preserveWhitespace()) {
                    return true;
                }
                el = el.parent();
                i++;
            } while (i < 6 && el != null);
        }
        return false;
    }

    static boolean lastCharIsWhitespace(StringBuilder sb) {
        return !sb.isEmpty() && sb.charAt(sb.length() - 1) == ' ';
    }
}
