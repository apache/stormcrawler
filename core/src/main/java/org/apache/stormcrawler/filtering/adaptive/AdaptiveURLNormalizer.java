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

package org.apache.stormcrawler.filtering.adaptive;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilter;
import org.apache.stormcrawler.util.URLUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Removes from URLs the query parameters which a site has shown to be irrelevant, based on the
 * canonical tags found in its pages. The aim is similar to the <i>Clean-param</i> extension of the
 * robots protocol by Yandex, except that the rules are learnt instead of being declared by the
 * site.
 *
 * <p><b>Without {@link org.apache.stormcrawler.parse.filter.CanonicalParamLearner} this filter has
 * nothing to apply</b>: the parsing bolts filter the outlinks of a page before running the parse
 * filters which extract its canonical tag, so the canonical is never in the metadata given here. The
 * learner gathers the evidence into the {@link CanonicalRules} instance named by <code>store</code>,
 * which also holds the configuration common to both.
 *
 * <p>Rules are learnt per JVM, so a URL discovered before a rule was established keeps the form it
 * was stored with and two workers may briefly disagree. Rules are only ever added, never withdrawn,
 * so the workers converge as the crawl progresses.
 *
 * @see <a href="https://github.com/apache/stormcrawler/issues/315">STORMCRAWLER-315</a>
 */
public class AdaptiveURLNormalizer extends URLFilter {

    static final String DEFAULT_STORE = "default";

    private CanonicalRules rules;

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode paramNode) {
        final JsonNode node = paramNode.get("store");
        final String store = node == null ? DEFAULT_STORE : node.asText(DEFAULT_STORE);
        rules = CanonicalRules.getInstance(stormConf, store);
    }

    @Override
    public @Nullable String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {
        // usually a no-op, but the canonical is there when the filters are called from a
        // parse filter or when the key is in metadata.persist
        if (sourceUrl != null && sourceMetadata != null) {
            rules.learn(sourceUrl, sourceMetadata.getFirstValue(rules.getCanonicalKey()));
        }
        return removeIrrelevantParams(urlToFilter);
    }

    /**
     * Rebuilds the URL without the parameters established as irrelevant for its site. Only the query
     * string is rewritten, everything else is copied verbatim, so that a URL which needed sanitizing
     * to be parsed is not silently replaced by its sanitized form.
     */
    private String removeIrrelevantParams(@NotNull String urlToFilter) {
        final int fragment = urlToFilter.indexOf('#');
        final int questionMark = urlToFilter.indexOf('?');
        if (questionMark == -1 || (fragment != -1 && fragment < questionMark)) {
            return urlToFilter;
        }
        final int queryEnd = fragment == -1 ? urlToFilter.length() : fragment;
        final String query = urlToFilter.substring(questionMark + 1, queryEnd);
        if (query.isEmpty()) {
            return urlToFilter;
        }

        final URL url;
        try {
            url = URLUtil.toURL(urlToFilter);
        } catch (MalformedURLException e) {
            // leave it to the filters in charge of the validity of the URLs
            return urlToFilter;
        }

        final String scopeKey = rules.scopeKey(url);
        if (scopeKey == null) {
            return urlToFilter;
        }

        final StringBuilder newQuery = new StringBuilder(query.length());
        boolean removedSomething = false;
        for (String param : query.split("&", -1)) {
            if (rules.isRemovable(scopeKey, CanonicalRules.parameterName(param))) {
                removedSomething = true;
                continue;
            }
            if (newQuery.length() > 0) {
                newQuery.append('&');
            }
            newQuery.append(param);
        }

        if (!removedSomething) {
            return urlToFilter;
        }

        final StringBuilder normalized = new StringBuilder(urlToFilter.length());
        normalized.append(urlToFilter, 0, questionMark);
        // http://example.com?a=b has no path: keep the same key as the other normalizers
        final String path = url.getPath();
        if (path == null || path.isEmpty()) {
            normalized.append('/');
        }
        if (newQuery.length() > 0) {
            normalized.append('?').append(newQuery);
        }
        normalized.append(urlToFilter, queryEnd, urlToFilter.length());
        return normalized.toString();
    }
}
