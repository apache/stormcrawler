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

package org.apache.stormcrawler.filtering.regex;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.InputSource;

/**
 * The RegexURLNormalizer is a URL filter that normalizes URLs by matching a regular expression and
 * inserting a replacement string.
 *
 * <p>Adapted from Apache Nutch 1.9.
 */
public class RegexURLNormalizer extends URLFilter {

    private static final Logger LOG = LoggerFactory.getLogger(RegexURLNormalizer.class);

    /** Class which holds a compiled pattern and its corresponding substitution string. */
    private static class Rule {
        public Pattern pattern;

        public String substitution;
    }

    private List<Rule> rules;

    private static final List<Rule> EMPTY_RULES = Collections.emptyList();

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode paramNode) {
        JsonNode node = paramNode.get("urlNormalizers");
        if (node != null && node.isArray()) {
            rules = readRules((ArrayNode) node);
        } else {
            JsonNode filenameNode = paramNode.get("regexNormalizerFile");
            String rulesFileName;
            if (filenameNode != null) {
                rulesFileName = filenameNode.textValue();
            } else {
                rulesFileName = "default-regex-normalizers.xml";
            }
            rules = readRules(rulesFileName);
        }
    }

    /**
     * This function does the replacements by iterating through all the regex patterns. It accepts a
     * string url as input and returns the altered string. If the normalized url is an empty string,
     * the function will return null.
     *
     * @param sourceUrl
     * @param sourceMetadata
     * @param urlString
     * @return
     */
    @Override
    public @Nullable String filter(
            @Nullable URL sourceUrl, @Nullable Metadata sourceMetadata, @NotNull String urlString) {

        for (Rule r : rules) {
            Matcher matcher = r.pattern.matcher(urlString);

            urlString = matcher.replaceAll(r.substitution);
        }

        if (urlString.equals("")) {
            urlString = null;
        }

        return urlString;
    }

    /** Populates a List of Rules off of JsonNode. */
    private List<Rule> readRules(ArrayNode rulesList) {
        List<Rule> rules = new ArrayList<>();
        for (JsonNode regexNode : rulesList) {
            if (regexNode == null || regexNode.isNull()) {
                LOG.warn("bad config: 'regex' element is null");
                continue;
            }
            JsonNode patternNode = regexNode.get("pattern");
            JsonNode substitutionNode = regexNode.get("substitution");

            String substitutionValue = "";
            if (substitutionNode != null) {
                substitutionValue = substitutionNode.asText();
            }
            if (patternNode != null && StringUtils.isNotBlank(patternNode.asText())) {
                Rule rule = createRule(patternNode.asText(), substitutionValue);
                if (rule != null) {
                    rules.add(rule);
                }
            }
        }
        if (rules.size() == 0) {
            rules = EMPTY_RULES;
        }
        return rules;
    }

    /** Reads the configuration file and populates a List of Rules. */
    private List<Rule> readRules(String rulesFile) {
        try {
            InputStream regexStream = getClass().getClassLoader().getResourceAsStream(rulesFile);
            if (regexStream == null) {
                LOG.error("Error loading rules from file: {}", rulesFile);
                return EMPTY_RULES;
            }
            Reader reader = new InputStreamReader(regexStream, StandardCharsets.UTF_8);
            return readConfiguration(reader);
        } catch (Exception e) {
            LOG.error("Error loading rules from file: {}", rulesFile, e);
            return EMPTY_RULES;
        }
    }

    private List<Rule> readConfiguration(Reader reader) {
        List<Rule> rules = new ArrayList<>();
        try {

            // borrowed heavily from code in Configuration.java
            Document doc =
                    DocumentBuilderFactory.newInstance()
                            .newDocumentBuilder()
                            .parse(new InputSource(reader));
            Element root = doc.getDocumentElement();
            if ((!"regex-normalize".equals(root.getTagName())) && (LOG.isErrorEnabled())) {
                LOG.error("bad conf file: top-level element not <regex-normalize>");
            }
            NodeList regexes = root.getChildNodes();
            for (int i = 0; i < regexes.getLength(); i++) {
                Node regexNode = regexes.item(i);
                if (!(regexNode instanceof Element)) {
                    continue;
                }
                Element regex = (Element) regexNode;
                if ((!"regex".equals(regex.getTagName())) && (LOG.isWarnEnabled())) {
                    LOG.warn("bad conf file: element not <regex>");
                }
                NodeList fields = regex.getChildNodes();
                String patternValue = null;
                String subValue = null;
                for (int j = 0; j < fields.getLength(); j++) {
                    Node fieldNode = fields.item(j);
                    if (!(fieldNode instanceof Element)) {
                        continue;
                    }
                    Element field = (Element) fieldNode;
                    if ("pattern".equals(field.getTagName()) && field.hasChildNodes()) {
                        patternValue = ((Text) field.getFirstChild()).getData();
                    }
                    if ("substitution".equals(field.getTagName()) && field.hasChildNodes()) {
                        subValue = ((Text) field.getFirstChild()).getData();
                    }
                    if (!field.hasChildNodes()) {
                        subValue = "";
                    }
                }
                if (patternValue != null && subValue != null) {
                    Rule rule = createRule(patternValue, subValue);
                    rules.add(rule);
                }
            }
        } catch (Exception e) {
            LOG.error("error parsing conf file", e);
            return EMPTY_RULES;
        }
        if (rules.size() == 0) {
            return EMPTY_RULES;
        }
        return rules;
    }

    private Rule createRule(String patternValue, String subValue) {
        Rule rule = new Rule();
        try {
            rule.pattern = Pattern.compile(patternValue);
        } catch (PatternSyntaxException e) {
            LOG.error(
                    "skipped rule: {} -> {} : invalid regular expression pattern" + patternValue,
                    subValue,
                    e);
            return null;
        }
        rule.substitution = subValue;
        return rule;
    }

    /**
     * Utility method to test rules against an input. the first arg is the absolute path of the
     * rules file, the second is the URL to be normalised
     */
    public static void main(String[] args) throws FileNotFoundException, IOException {
        RegexURLNormalizer normalizer = new RegexURLNormalizer();
        normalizer.rules =
                normalizer.readConfiguration(new FileReader(args[0], StandardCharsets.UTF_8));

        String output = normalizer.filter(null, null, args[1]);

        System.out.println(args[1] + "\n->\n" + output);
    }
}
