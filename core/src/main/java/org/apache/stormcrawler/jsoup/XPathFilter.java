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

package org.apache.stormcrawler.jsoup;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.JSoupFilter;
import org.apache.stormcrawler.parse.ParseData;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.stormcrawler.util.AbstractConfigurable;
import org.jetbrains.annotations.NotNull;
import org.jsoup.helper.W3CDom;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.NodeList;

/** Reads a XPATH patterns and stores the value found in web page as metadata. */
public class XPathFilter extends AbstractConfigurable implements JSoupFilter {

    private static final Logger LOG = LoggerFactory.getLogger(XPathFilter.class);

    protected final Map<String, List<LabelledExpression>> expressions = new HashMap<>();

    private final XPath xpath = XPathFactory.newInstance().newXPath();

    /**
     * Supported extraction functions that can be appended to XPath expressions. These provide
     * backward compatibility with the non-standard XSoup functions.
     */
    enum EvalFunction {
        /** Extracts cleaned, whitespace-normalized text from the element. */
        TIDY_TEXT,
        /** Extracts all text content from the element (same as TIDY_TEXT for JSoup). */
        ALL_TEXT,
        /** Extracts the inner HTML of the element. */
        HTML,
        /** Extracts an attribute value from the element. */
        ATTR,
        /** Returns the element's own text representation. */
        NONE;

        String evaluate(Element element, String attrName) {
            switch (this) {
                case TIDY_TEXT:
                case ALL_TEXT:
                    return element.text();
                case HTML:
                    return element.html();
                case ATTR:
                    return element.attr(attrName);
                default:
                    return element.text();
            }
        }
    }

    /** Pattern to match trailing function calls like /tidyText(), /html(), /allText(). */
    private static final Pattern FUNCTION_SUFFIX =
            Pattern.compile("/(tidyText|allText|html)\\(\\)$");

    /** Pattern to match trailing attribute selectors like /@content. */
    private static final Pattern ATTR_SUFFIX = Pattern.compile("/@([\\w-]+)$");

    /**
     * Pattern matching XPath element names (e.g. SPAN in //SPAN[@class="x"]). Matches sequences of
     * word characters that follow / or // and are not attribute references (/@).
     */
    private static final Pattern ELEMENT_NAME = Pattern.compile("(?<=/)(?!@)([A-Z][A-Za-z0-9]*)");

    /**
     * Lowercases element names in an XPath expression to match JSoup's normalized tag names. For
     * example, {@code //SPAN[@class="concept"]} becomes {@code //span[@class="concept"]}.
     */
    public static String lowercaseElementNames(String xpath) {
        return ELEMENT_NAME
                .matcher(xpath)
                .replaceAll(m -> m.group().toLowerCase(java.util.Locale.ROOT));
    }

    /**
     * W3C representation of a JSoup document, built once per page and shared by all the
     * expressions. Element.selectXpath() would otherwise convert the whole document on every call.
     */
    static class W3CContext {

        private final W3CDom w3c = new W3CDom().namespaceAware(false);
        private final org.w3c.dom.Node contextNode;

        W3CContext(Document doc) {
            this.contextNode = w3c.contextNode(w3c.fromJsoup(doc));
        }
    }

    static class LabelledExpression {

        String key;
        private String xpath;
        private XPathExpression expression;
        private EvalFunction evalFunction;
        private String attrName;

        private LabelledExpression(String key, String xpath, XPath compiler)
                throws XPathExpressionException {
            this.key = key;
            parseExpression(xpath);
            this.expression = compiler.compile(this.xpath);
        }

        private void parseExpression(String rawXpath) {
            // Lowercase element names so that XPath expressions with uppercase
            // tag names (e.g. //SPAN) work with JSoup's lowercase-normalized DOM
            rawXpath = lowercaseElementNames(rawXpath);

            // Check for custom function suffixes: /tidyText(), /allText(), /html()
            Matcher funcMatcher = FUNCTION_SUFFIX.matcher(rawXpath);
            if (funcMatcher.find()) {
                this.xpath = rawXpath.substring(0, funcMatcher.start());
                switch (funcMatcher.group(1)) {
                    case "tidyText":
                        this.evalFunction = EvalFunction.TIDY_TEXT;
                        break;
                    case "allText":
                        this.evalFunction = EvalFunction.ALL_TEXT;
                        break;
                    case "html":
                        this.evalFunction = EvalFunction.HTML;
                        break;
                    default:
                        this.evalFunction = EvalFunction.NONE;
                }
                return;
            }

            // Check for attribute selectors: /@attrName
            Matcher attrMatcher = ATTR_SUFFIX.matcher(rawXpath);
            if (attrMatcher.find()) {
                this.xpath = rawXpath.substring(0, attrMatcher.start());
                this.evalFunction = EvalFunction.ATTR;
                this.attrName = attrMatcher.group(1);
                return;
            }

            // No special suffix — use as-is
            this.xpath = rawXpath;
            this.evalFunction = EvalFunction.NONE;
        }

        List<String> evaluate(W3CContext context) throws XPathExpressionException {
            NodeList nodes =
                    (NodeList) expression.evaluate(context.contextNode, XPathConstants.NODESET);
            List<Element> elements = context.w3c.sourceNodes(nodes, Element.class);
            List<String> results = new ArrayList<>();
            for (Element element : elements) {
                String value = evalFunction.evaluate(element, attrName);
                // blank values are skipped so that the next expression can be tried
                if (value != null && !value.isBlank()) {
                    results.add(value);
                }
            }
            return results;
        }

        public String toString() {
            return key + ":" + xpath;
        }
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        super.configure(stormConf, filterParams);
        java.util.Iterator<Entry<String, JsonNode>> iter = filterParams.fields();
        while (iter.hasNext()) {
            Entry<String, JsonNode> entry = iter.next();
            String key = entry.getKey();
            JsonNode node = entry.getValue();
            if (node.isArray()) {
                for (JsonNode expression : node) {
                    addExpression(key, expression);
                }
            } else {
                addExpression(key, entry.getValue());
            }
        }
    }

    private void addExpression(String key, JsonNode expression) {
        String xpathvalue = expression.asText();
        try {
            expressions
                    .computeIfAbsent(key, k -> new ArrayList<>())
                    .add(new LabelledExpression(key, xpathvalue, xpath));
        } catch (Exception e) {
            throw new RuntimeException("Can't compile expression : " + xpathvalue, e);
        }
    }

    /**
     * Converts the JSoup document into a W3C DOM against which the expressions are evaluated.
     * Returns null if there are no expressions or the conversion failed.
     */
    W3CContext toW3C(String url, Document doc) {
        if (expressions.isEmpty()) {
            return null;
        }
        try {
            return new W3CContext(doc);
        } catch (Exception e) {
            LOG.error("Error converting {} to W3C DOM", url, e);
            return null;
        }
    }

    @Override
    public void filter(
            String url, byte[] content, org.jsoup.nodes.Document doc, ParseResult parse) {

        W3CContext context = toW3C(url, doc);
        if (context == null) {
            return;
        }

        ParseData parseData = parse.getOrCreate(url);
        Metadata metadata = parseData.getMetadata();

        // applies the XPATH expression in the order in which they are produced
        for (List<LabelledExpression> leList : expressions.values()) {
            for (LabelledExpression le : leList) {
                try {
                    List<String> values = le.evaluate(context);
                    if (values != null && !values.isEmpty()) {
                        metadata.addValues(le.key, values);
                        break;
                    }
                } catch (Exception e) {
                    LOG.error("Error evaluating {} on {}", le.key, url, e);
                }
            }
        }
    }
}
