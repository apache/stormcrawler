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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.xml.namespace.QName;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.ParseData;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.xml.serialize.Method;
import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Simple ParseFilter to illustrate and test the interface. Reads a XPATH pattern from the config
 * file and stores the value as metadata
 */
public class XPathFilter extends ParseFilter {

    private enum EvalFunction {
        NONE,
        STRING,
        SERIALIZE;

        public QName getReturnType() {
            switch (this) {
                case STRING:
                    return XPathConstants.STRING;
                default:
                    return XPathConstants.NODESET;
            }
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(XPathFilter.class);

    private XPathFactory factory = XPathFactory.newInstance();
    private XPath xpath = factory.newXPath();

    protected final Map<String, List<LabelledExpression>> expressions = new HashMap<>();

    class LabelledExpression {

        String key;
        private EvalFunction evalFunction;
        private XPathExpression expression;

        private LabelledExpression(String key, String expression) throws XPathExpressionException {
            this.key = key;
            if (expression.startsWith("string(")) {
                evalFunction = EvalFunction.STRING;
            } else if (expression.startsWith("serialize(")) {
                expression = expression.substring(10, expression.length() - 1);
                evalFunction = EvalFunction.SERIALIZE;
            } else {
                evalFunction = EvalFunction.NONE;
            }
            this.expression = xpath.compile(expression);
        }

        List<String> evaluate(DocumentFragment doc) throws XPathExpressionException, IOException {
            Object evalResult = expression.evaluate(doc, evalFunction.getReturnType());
            List<String> values = new LinkedList<>();
            switch (evalFunction) {
                case STRING:
                    if (evalResult != null) {
                        String strippedValue = StringUtils.strip((String) evalResult);
                        values.add(strippedValue);
                    }
                    break;
                case SERIALIZE:
                    NodeList nodesToSerialize = (NodeList) evalResult;
                    StringWriter out = new StringWriter();
                    OutputFormat format = new OutputFormat(Method.XHTML, null, false);
                    format.setOmitXMLDeclaration(true);
                    XMLSerializer serializer = new XMLSerializer(out, format);
                    for (int i = 0; i < nodesToSerialize.getLength(); i++) {
                        Node node = nodesToSerialize.item(i);
                        switch (node.getNodeType()) {
                            case Node.ELEMENT_NODE:
                                serializer.serialize((Element) node);
                                break;
                            case Node.DOCUMENT_NODE:
                                serializer.serialize((Document) node);
                                break;
                            case Node.DOCUMENT_FRAGMENT_NODE:
                                serializer.serialize((DocumentFragment) node);
                                break;
                            case Node.TEXT_NODE:
                                String text = node.getTextContent();
                                if (text.length() > 0) {
                                    values.add(text);
                                }
                                // By pass the rest of the code since it is used to
                                // extract
                                // the value out of the serialized which isn't used in
                                // this case
                                continue;
                        }
                        String serializedValue = out.toString();
                        if (serializedValue.length() > 0) {
                            values.add(serializedValue);
                        }
                        out.getBuffer().setLength(0);
                    }
                    break;
                default:
                    NodeList nodes = (NodeList) evalResult;
                    for (int i = 0; i < nodes.getLength(); i++) {
                        Node node = nodes.item(i);
                        values.add(StringUtils.strip(node.getTextContent()));
                    }
            }
            return values;
        }
    }

    @Override
    public void filter(String URL, byte[] content, DocumentFragment doc, ParseResult parse) {

        ParseData parseData = parse.get(URL);
        Metadata metadata = parseData.getMetadata();

        // applies the XPATH expression in the order in which they are produced
        for (List<LabelledExpression> leList : expressions.values()) {
            for (LabelledExpression le : leList) {
                try {
                    List<String> values = le.evaluate(doc);
                    if (values != null && !values.isEmpty()) {
                        metadata.addValues(le.key, values);
                        break;
                    }
                } catch (XPathExpressionException | IOException e) {
                    LOG.error("Error evaluating {}: {}", le.key, e);
                }
            }
        }
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
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
            List<LabelledExpression> lexpressionList = expressions.get(key);
            if (lexpressionList == null) {
                lexpressionList = new ArrayList<>();
                expressions.put(key, lexpressionList);
            }
            LabelledExpression lexpression = new LabelledExpression(key, xpathvalue);
            lexpressionList.add(lexpression);
        } catch (XPathExpressionException e) {
            throw new RuntimeException("Can't compile expression : " + xpathvalue, e);
        }
    }

    @Override
    public boolean needsDOM() {
        return true;
    }
}
