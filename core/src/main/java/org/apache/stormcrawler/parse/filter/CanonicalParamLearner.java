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
import java.net.MalformedURLException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.stormcrawler.filtering.adaptive.AdaptiveURLNormalizer;
import org.apache.stormcrawler.filtering.adaptive.CanonicalRules;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.stormcrawler.util.URLUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

/**
 * Compares the URL of a page with the value of its canonical tag and records which of its query
 * parameters the site considers irrelevant, so that {@link AdaptiveURLNormalizer} can remove them
 * from the URLs of that site.
 *
 * <p>Must run <b>after</b> the filter extracting the canonical tag into the metadata, typically an
 * <code>XPathFilter</code> configured with <code>"canonical": "//*[@rel=\"canonical\"]/@href"</code>
 * . Only reads the parse result, never modifies it.
 *
 * @see CanonicalRules for the configuration, which is shared with the URL filter
 * @see <a href="https://github.com/apache/stormcrawler/issues/315">STORMCRAWLER-315</a>
 */
public class CanonicalParamLearner extends ParseFilter {

    private static final Logger LOG = LoggerFactory.getLogger(CanonicalParamLearner.class);

    private CanonicalRules rules;

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode paramNode) {
        final JsonNode node = paramNode.get("store");
        final String store = node == null ? "default" : node.asText("default");
        rules = CanonicalRules.getInstance(stormConf, store);
    }

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {
        // getValues rather than get(url), which would insert an empty ParseData
        final String[] canonicals = parse.getValues(url, rules.getCanonicalKey());
        if (canonicals == null || canonicals.length == 0) {
            return;
        }
        final String canonical = canonicals[0];
        if (StringUtils.isBlank(canonical)) {
            return;
        }
        try {
            rules.learn(URLUtil.toURL(url), canonical);
        } catch (MalformedURLException e) {
            LOG.debug("Unable to parse {} while learning from its canonical tag", url);
        }
    }
}
