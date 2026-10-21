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
import org.apache.stormcrawler.filtering.adaptive.CanonicalRules.QueryToken;
import org.apache.stormcrawler.util.URLUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Removes from URLs the query parameters which a site has shown to be irrelevant, based on the
 * canonical tags found in its pages. The aim is similar to the <i>Clean-param</i> extension of the
 * robots protocol by Yandex, except that the rules are learnt instead of being declared by the
 * site.
 *
 * <p><b>This filter does not learn anything itself</b>: the parsing bolts filter the outlinks of a
 * page before running the parse filters which extract its canonical tag, so the metadata given here
 * can at best hold the canonical of a previous fetch. The evidence is gathered by {@link
 * org.apache.stormcrawler.parse.filter.CanonicalParamLearner} into the {@link CanonicalRules} of
 * the JVM, which also hold the configuration common to both. Best declared after the <code>
 * BasicURLNormalizer</code>, so that the query string it sees is already tidy.
 *
 * <p>A redirection whose target normalises back to its source, such as <code>/c</code> redirecting
 * to <code>/c?sid=x</code>, is left as it is: normalised, it would be dropped as a link to itself
 * and the content behind it never fetched.
 *
 * <p>Rules are learnt per JVM: a URL discovered before a rule was established keeps the form it was
 * stored with, and a worker running no parser executor never learns, hence never normalises. Rules
 * are never withdrawn, unless their host gets evicted from the bounded cache of evidence, so the
 * workers which do learn converge as the crawl progresses.
 *
 * @see <a href="https://github.com/apache/stormcrawler/issues/315">STORMCRAWLER-315</a>
 */
public class AdaptiveURLNormalizer extends URLFilter {

    /** Set on the metadata by the fetcher bolts before a redirection goes through the filters. */
    private static final String REDIRECTION_KEY = "_redirTo";

    private CanonicalRules rules;

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode paramNode) {
        rules = CanonicalRules.getInstance(stormConf);
    }

    @Override
    public void cleanup() {
        if (rules != null) {
            rules.release();
            rules = null;
        }
    }

    @Override
    public @Nullable String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {
        final String normalized = removeIrrelevantParams(urlToFilter);
        if (normalized.equals(urlToFilter)) {
            return urlToFilter;
        }
        if (sourceUrl != null
                && sourceMetadata != null
                && sourceMetadata.containsKey(REDIRECTION_KEY)
                && withoutFragment(normalized)
                        .equals(withoutFragment(sourceUrl.toExternalForm()))) {
            // the site sent us from /c to /c?sid=x: the only way to the content is with the param
            return urlToFilter;
        }
        return normalized;
    }

    /**
     * Rebuilds the URL without the parameters established as irrelevant for its site. Only the
     * query string is rewritten, everything else is copied verbatim, so that a URL which needed
     * sanitizing to be parsed is not silently replaced by its sanitized form.
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
        final int pathHash = CanonicalRules.pathHash(url);

        final StringBuilder newQuery = new StringBuilder(query.length());
        boolean removedSomething = false;
        for (QueryToken token : CanonicalRules.tokenize(query)) {
            if (rules.isRemovable(scopeKey, pathHash, token.name())) {
                removedSomething = true;
                continue;
            }
            // each surviving pair keeps the separator which preceded it, & or ;
            if (newQuery.length() > 0) {
                newQuery.append(token.separator());
            }
            newQuery.append(token.raw());
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

    private static String withoutFragment(String url) {
        final int fragment = url.indexOf('#');
        return fragment == -1 ? url : url.substring(0, fragment);
    }
}
