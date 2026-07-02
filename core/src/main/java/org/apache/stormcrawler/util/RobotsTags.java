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

package org.apache.stormcrawler.util;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.stormcrawler.Metadata;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Normalises the robots instructions provided by the HTML meta tags or the HTTP X-Robots-Tag
 * headers.
 */
public class RobotsTags {

    public static final String ROBOTS_NO_INDEX = "robots.noIndex";

    public static final String ROBOTS_NO_FOLLOW = "robots.noFollow";

    /**
     * Whether to interpret the noFollow directive strictly (remove links) or not (remove anchor and
     * do not track original URL). True by default.
     */
    public static final String ROBOTS_NO_FOLLOW_STRICT = "robots.noFollow.strict";

    public static final String ROBOTS_NO_CACHE = "robots.noCache";

    private boolean noIndex = false;

    private boolean noFollow = false;

    private boolean noCache = false;

    private static final XPathExpression expression;

    static {
        XPath xpath = XPathFactory.newInstance().newXPath();
        try {
            expression = xpath.compile("/HTML/*/META");
        } catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
    }

    /** Get the values from the fetch metadata. */
    public RobotsTags(Metadata metadata, String protocolMDprefix) {
        // HTTP headers
        // X-Robots-Tag: noindex
        String[] values = metadata.getValues("X-Robots-Tag", protocolMDprefix);
        if (values == null) {
            return;
        }
        if (values.length == 1) {
            // just in case they put all the values on a single line
            values = values[0].split(" *, *");
        }
        parseValues(values);
    }

    public RobotsTags() {}

    // set the values based on the meta tags
    // HTML tags
    // <meta name="robots" content="noarchive, nofollow"/>
    // called by the parser bolts
    public void extractMetaTags(DocumentFragment doc) throws XPathExpressionException {
        NodeList nodes = (NodeList) expression.evaluate(doc, XPathConstants.NODESET);
        if (nodes == null) {
            return;
        }
        int numNodes = nodes.getLength();
        for (int i = 0; i < numNodes; i++) {
            Node n = (Node) nodes.item(i);
            // iterate on the attributes
            // and check that it has name=robots and content
            // whatever the case is
            boolean isRobots = false;
            String content = null;
            NamedNodeMap attrs = n.getAttributes();
            for (int att = 0; att < attrs.getLength(); att++) {
                Node keyval = attrs.item(att);
                if ("name".equalsIgnoreCase(keyval.getNodeName())
                        && "robots".equalsIgnoreCase(keyval.getNodeValue())) {
                    isRobots = true;
                    continue;
                }
                if ("content".equalsIgnoreCase(keyval.getNodeName())) {
                    content = keyval.getNodeValue();
                    continue;
                }
            }

            if (isRobots && content != null) {
                // got a value - split it
                String[] vals = content.split(" *, *");
                parseValues(vals);
                return;
            }
        }
    }

    /** Extracts meta tags based on the value of the content attribute. */
    public void extractMetaTags(String content) {
        if (content == null) {
            return;
        }
        String[] vals = content.split(" *, *");
        parseValues(vals);
    }

    private void parseValues(String[] values) {
        for (String v : values) {
            v = v.trim();
            if ("noindex".equalsIgnoreCase(v)) {
                noIndex = true;
            } else if ("nofollow".equalsIgnoreCase(v)) {
                noFollow = true;
            } else if ("noarchive".equalsIgnoreCase(v)) {
                noCache = true;
            } else if ("none".equalsIgnoreCase(v)) {
                noIndex = true;
                noFollow = true;
                noCache = true;
            }
        }
    }

    /** Adds a normalised representation of the directives in the metadata. */
    public void normaliseToMetadata(Metadata metadata) {
        metadata.setValue(ROBOTS_NO_INDEX, Boolean.toString(noIndex));
        metadata.setValue(ROBOTS_NO_CACHE, Boolean.toString(noCache));
        metadata.setValue(ROBOTS_NO_FOLLOW, Boolean.toString(noFollow));
    }

    public boolean isNoIndex() {
        return noIndex;
    }

    public boolean isNoFollow() {
        return noFollow;
    }

    public boolean isNoCache() {
        return noCache;
    }
}
