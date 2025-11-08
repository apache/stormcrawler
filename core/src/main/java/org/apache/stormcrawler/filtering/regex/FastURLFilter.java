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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.stormcrawler.JSONResource;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilter;
import org.apache.stormcrawler.util.ConfUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * URL filter based on regex patterns and organised by [host | domain | metadata | global]. For a
 * given URL, the scopes are tried in the order given above and the URL is kept or removed based on
 * the first matching rule. The default policy is to accept a URL if no matches are found.
 *
 * <p>The resource file is in JSON and at the following format.
 *
 * <pre>
 * {
 *  "rules" : [ {
 *   "scope" : "GLOBAL",
 *    "patterns" : [ "DenyPathQuery \\.jpg" ]
 *  }, {
 *    "scope" : "domain:stormcrawler.net",
 *    "patterns" : [ "AllowPath /stormcrawler/", "DenyPath .+" ]
 *  }, {
 *    "scope" : "metadata:key=value",
 *   "patterns" : [ "DenyPath .+" ]
 *  } ]
 * }
 * </pre>
 *
 * Partly inspired by https://github.com/commoncrawl/nutch/blob/cc-fast-url-filter
 * /src/plugin/urlfilter -fast/src/java/org/apache/nutch/urlfilter/fast/FastURLFilter.java
 */
public class FastURLFilter extends URLFilter implements JSONResource {

    public static final Logger LOG = LoggerFactory.getLogger(FastURLFilter.class);

    private String resourceFile;

    private Rules rules = new Rules();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {

        if (filterParams != null) {
            JsonNode node = filterParams.get("file");
            if (node != null && node.isTextual()) {
                this.resourceFile = node.asText("fast.urlfilter.json");
            }
        }

        // config via json failed - trying from global config
        if (this.resourceFile == null) {
            this.resourceFile =
                    ConfUtils.getString(stormConf, "fast.urlfilter.file", "fast.urlfilter.json");
        }

        try {
            loadJSONResources();
        } catch (Exception e) {
            LOG.error("Exception while loading JSON resources from jar", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getResourceFile() {
        return resourceFile;
    }

    @Override
    public void loadJSONResources(InputStream inputStream)
            throws JsonParseException, JsonMappingException, IOException {

        JsonNode rootNode = objectMapper.readTree(inputStream);

        // if it contains a single object
        // jump directly to its content
        // https://github.com/apache/stormcrawler/issues/1013
        if (rootNode.size() == 1 && rootNode.isObject()) {
            rootNode = rootNode.fields().next().getValue();
        }

        final Rules rules = new Rules();

        Iterator<JsonNode> iter = rootNode.elements();
        while (iter.hasNext()) {
            final JsonNode current = iter.next();
            final Scope scope = new Scope();
            final String scopeval = current.get("scope").asText().trim();
            int offset = 0;
            Scope.Type type;
            String value = null;
            // separate the type from the pattern
            if (scopeval.equals("GLOBAL")) {
                type = Scope.Type.GLOBAL;
            } else if (scopeval.startsWith("domain:")) {
                type = Scope.Type.DOMAIN;
                offset = "domain:".length();
                value = scopeval.substring(offset);
            } else if (scopeval.startsWith("host:")) {
                type = Scope.Type.HOSTNAME;
                offset = "host:".length();
                value = scopeval.substring(offset);
            } else if (scopeval.startsWith("metadata:")) {
                type = Scope.Type.METADATA;
                offset = "metadata:".length();
                value = scopeval.substring(offset);
            } else {
                throw new RuntimeException("Invalid scope: " + scopeval);
            }

            final JsonNode patternsNode = current.get("patterns");
            if (patternsNode == null) {
                throw new RuntimeException("Missing patterns for scope" + scopeval);
            }

            final List<Rule> rlist = new LinkedList<>();

            Iterator<JsonNode> iterPatterns = patternsNode.elements();
            while (iterPatterns.hasNext()) {
                JsonNode patternNode = iterPatterns.next();
                rlist.add(new Rule(patternNode.asText()));
            }

            scope.setRules(rlist);

            rules.addScope(scope, type, value);
        }

        this.rules = rules;
    }

    @Override
    public @Nullable String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {
        try {
            if (rules.filter(urlToFilter, sourceMetadata)) {
                return null;
            }
        } catch (MalformedURLException e) {
            return null;
        }
        return urlToFilter;
    }
}

class Rules {

    private Scope globalRules;
    private Map<String, Scope> domainRules = new HashMap<>();
    private Map<String, Scope> hostNameRules = new HashMap<>();
    private List<MDScope> metadataRules = new ArrayList<>();

    public void addScope(Scope s, Scope.Type t, String value) {
        if (t.equals(Scope.Type.GLOBAL)) {
            globalRules = s;
        } else if (t.equals(Scope.Type.DOMAIN)) {
            domainRules.put(value, s);
        } else if (t.equals(Scope.Type.HOSTNAME)) {
            hostNameRules.put(value, s);
        } else if (t.equals(Scope.Type.METADATA)) {
            metadataRules.add(new MDScope(value, s.getRules()));
        }
    }

    /**
     * Try the rules from the hostname, domain name, metadata and global scopes in this order.
     * Returns true if the URL should be removed, false otherwise. The value returns the value of
     * the first matching rule, be it positive or negative.
     *
     * @throws MalformedURLException
     */
    public boolean filter(String url, Metadata metadata) throws MalformedURLException {
        URL u = new URL(url);

        // first try the full hostname
        String hostname = u.getHost();
        if (checkScope(hostNameRules.get(hostname), u)) {
            return true;
        }

        // then on the various components of the domain
        final String[] domainParts = hostname.split("\\.");
        String domain = null;
        for (int i = domainParts.length - 1; i >= 0; i--) {
            domain = domainParts[i] + (domain == null ? "" : "." + domain);
            if (checkScope(domainRules.get(domain), u)) {
                return true;
            }
        }

        // check on parent's URL metadata
        for (MDScope scope : metadataRules) {
            final String[] vals = metadata.getValues(scope.getKey());
            if (vals == null) {
                continue;
            }
            for (String v : vals) {
                if (v.equalsIgnoreCase(scope.getValue())) {
                    FastURLFilter.LOG.debug(
                            "Filtering {} matching metadata {}:{}",
                            url,
                            scope.getKey(),
                            scope.getValue());
                    if (checkScope(scope, u)) {
                        return true;
                    }
                }
            }
        }

        if (checkScope(globalRules, u)) {
            return true;
        }

        return false;
    }

    private boolean checkScope(Scope s, URL u) {
        if (s == null) {
            return false;
        }
        for (Rule r : s.getRules()) {
            String haystack = u.getPath();
            // whether to include the query as well?
            if (r.getType().toString().endsWith("QUERY")) {
                if (u.getQuery() != null) {
                    haystack += "?" + u.getQuery();
                }
            }
            if (r.getPattern().matcher(haystack).find()) {
                // matches! returns true for DENY, false for ALLOW
                return r.getType().toString().startsWith("DENY");
            }
        }
        return false;
    }
}

class Scope {

    public enum Type {
        DOMAIN,
        GLOBAL,
        HOSTNAME,
        METADATA
    };

    protected Rule[] rules;

    public void setRules(List<Rule> rlist) {
        this.rules = rlist.toArray(new Rule[0]);
    }

    public Rule[] getRules() {
        return rules;
    }
}

class MDScope extends Scope {

    private String key;
    private String value;

    MDScope(String constraint, Rule[] rules) {
        this.rules = rules;
        int eq = constraint.indexOf("=");
        if (eq != -1) {
            key = constraint.substring(0, eq);
            value = constraint.substring(eq + 1);
        } else {
            key = constraint;
        }
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}

class Rule {

    public enum Type {
        DENYPATH,
        DENYPATHQUERY,
        ALLOWPATH,
        ALLOWPATHQUERY
    };

    private Type type;
    private Pattern pattern;

    public Rule(String line) {
        int offset = 0;
        String lcline = line.toLowerCase(Locale.ROOT);
        // separate the type from the pattern
        for (Type t : Type.values()) {
            String start = t.toString().toLowerCase(Locale.ROOT) + " ";
            if (lcline.startsWith(start)) {
                type = t;
                offset = start.length();
                break;
            }
        }
        // no match?
        if (type == null) {
            return;
        }

        String patternString = line.substring(offset).trim();
        pattern = Pattern.compile(patternString);
    }

    public Type getType() {
        return type;
    }

    public Pattern getPattern() {
        return pattern;
    }
}
