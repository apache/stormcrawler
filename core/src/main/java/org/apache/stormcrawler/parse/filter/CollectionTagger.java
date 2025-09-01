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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.stormcrawler.JSONResource;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.stormcrawler.util.ConfUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

/**
 * Assigns one or more tags to the metadata of a document based on its URL matching patterns defined
 * in a JSON resource file.
 *
 * <p>The resource file must specify regular expressions for inclusions but also for exclusions e.g.
 *
 * <pre>
 * {
 *   "collections": [{
 *            "name": "stormcrawler",
 *            "includePatterns": ["http://stormcrawler.net/.+"]
 *        },
 *        {
 *            "name": "crawler",
 *            "includePatterns": [".+crawler.+", ".+nutch.+"],
 *            "excludePatterns": [".+baby.+", ".+spider.+"]
 *        }
 *    ]
 * }
 * </pre>
 *
 * @see <a href=
 *     "https://www.google.com/support/enterprise/static/gsa/docs/admin/74/admin_console_help/crawl_collections.html">collections
 *     in Google Search Appliance</a>
 *     <p>This resources was kindly donated by the Government of Northwestern Territories in Canada
 *     (http://www.gov.nt.ca/).
 */
public class CollectionTagger extends ParseFilter implements JSONResource {

    private static final Logger LOG = LoggerFactory.getLogger(CollectionTagger.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final TypeReference<Collections> reference = new TypeReference<>() {};

    private Collections collections = new Collections();

    private String key;
    private String resourceFile;

    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {

        if (filterParams != null) {
            JsonNode node = filterParams.get("key");
            if (node != null && node.isTextual()) {
                this.key = node.asText("collections");
            }
            node = filterParams.get("file");
            if (node != null && node.isTextual()) {
                this.resourceFile = node.asText("collections.json");
            }
        }

        // config via json failed - trying from global config
        if (this.key == null) {
            this.key = ConfUtils.getString(stormConf, "collections.key", "collections");
        }
        if (this.resourceFile == null) {
            this.resourceFile =
                    ConfUtils.getString(stormConf, "collections.file", "collections.json");
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
        collections = (Collections) objectMapper.readValue(inputStream, reference);
    }

    @Override
    public void filter(String URL, byte[] content, DocumentFragment doc, ParseResult parse) {
        String[] tags = collections.tag(URL);
        if (tags.length > 0) {
            parse.get(URL).getMetadata().setValues(key, tags);
        }
    }
}

class Collections {

    private Set<Collection> collections;

    public void setCollections(Set<Collection> collections) {
        this.collections = collections;
    }

    public String[] tag(String url) {
        Set<String> tags = new HashSet<>();
        for (Collection collection : collections) {
            if (collection.matches(url)) {
                tags.add(collection.getName());
            }
        }
        return tags.toArray(new String[0]);
    }
}

class Collection {

    private String name;
    private Set<Pattern> includePatterns;
    private Set<Pattern> excludePatterns;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return true if the URL matches a pattern for this collection and no exclusion patterns
     */
    public boolean matches(String url) {
        boolean matches = false;
        for (Pattern includeP : includePatterns) {
            Matcher m = includeP.matcher(url);
            if (m.matches()) {
                matches = true;
                break;
            }
        }
        // no match
        if (!matches) {
            return false;
        }

        if (excludePatterns == null) {
            return true;
        }

        // check for antipatterns
        for (Pattern excludeP : excludePatterns) {
            Matcher m = excludeP.matcher(url);
            if (m.matches()) {
                return false;
            }
        }

        return true;
    }

    public void setIncludePatterns(Set<Pattern> includePatterns) {
        this.includePatterns = includePatterns;
    }

    public void setExcludePatterns(Set<Pattern> excludePatterns) {
        this.excludePatterns = excludePatterns;
    }
}
