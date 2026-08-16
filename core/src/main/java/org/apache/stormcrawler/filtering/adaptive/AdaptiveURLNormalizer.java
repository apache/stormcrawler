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
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import crawlercommons.domains.PaidLevelDomain;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilter;
import org.apache.stormcrawler.util.URLUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Normalizes URLs by removing the query parameters which a site has shown to be irrelevant, based
 * on the canonical tags found in its pages.
 *
 * <p>Whenever a page is parsed, this filter compares the URL of the page with the value of its
 * canonical tag (as extracted into the metadata, see <code>canonicalMetadataKey</code>). When both
 * point at the same resource - same protocol, host, port and path - and differ only by their query
 * string, the parameters dropped by the canonical are taken as evidence that they do not affect the
 * content, whereas the ones kept by the canonical are evidence of the opposite.
 *
 * <p>Once enough evidence has been gathered for a given parameter, subsequent URLs for that site
 * get the parameter removed, which reduces the amount of duplicates fetched. The aim is similar to
 * the <i>Clean-param</i> extension of the robots protocol by Yandex, except that the rules are
 * learnt instead of being declared by the site.
 *
 * <p>The evidence is kept in memory only and is therefore lost when the topology is restarted; it
 * is not shared between the instances of the bolt either. Both the number of sites and the number
 * of parameters tracked per site are bounded, see <code>maxScopes</code> and <code>maxParams</code>
 * .
 *
 * <p>Configuration, all parameters are optional:
 *
 * <pre>{@code
 * {
 *   "class": "org.apache.stormcrawler.filtering.adaptive.AdaptiveURLNormalizer",
 *   "name": "AdaptiveURLNormalizer",
 *   "params": {
 *     "canonicalMetadataKey": "canonical",
 *     "scope": "host",
 *     "minObservations": 5,
 *     "confidenceThreshold": 0.9,
 *     "maxScopes": 10000,
 *     "maxParams": 100
 *   }
 * }
 * }</pre>
 *
 * @see <a href="https://github.com/apache/stormcrawler/issues/315">STORMCRAWLER-315</a>
 */
public class AdaptiveURLNormalizer extends URLFilter {

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveURLNormalizer.class);

    /** Metadata key under which the canonical tag is stored by default. */
    private static final String DEFAULT_CANONICAL_KEY = "canonical";

    private static final int DEFAULT_MIN_OBSERVATIONS = 5;

    private static final double DEFAULT_CONFIDENCE_THRESHOLD = 0.9d;

    private static final int DEFAULT_MAX_SCOPES = 10_000;

    private static final int DEFAULT_MAX_PARAMS = 100;

    private String canonicalMetadataKey = DEFAULT_CANONICAL_KEY;

    private int minObservations = DEFAULT_MIN_OBSERVATIONS;

    private double confidenceThreshold = DEFAULT_CONFIDENCE_THRESHOLD;

    private int maxParams = DEFAULT_MAX_PARAMS;

    /** Whether the rules are learnt per domain instead of per host. */
    private boolean scopeByDomain = false;

    /** Evidence gathered so far, keyed by host or domain. */
    private Cache<String, Map<String, ParamStats>> stats = buildCache(DEFAULT_MAX_SCOPES);

    /**
     * URL of the page the last observation was made from. The filter is called once per outlink,
     * i.e. many times in a row with the same source, but a page must only count once.
     */
    private String lastLearnedSource;

    private static Cache<String, Map<String, ParamStats>> buildCache(int maxScopes) {
        return Caffeine.newBuilder().maximumSize(maxScopes).build();
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode paramNode) {
        int maxScopes = DEFAULT_MAX_SCOPES;

        JsonNode node = paramNode.get("canonicalMetadataKey");
        if (node != null) {
            final String key = node.asText();
            if (StringUtils.isBlank(key)) {
                LOG.warn("Ignoring blank value for canonicalMetadataKey");
            } else {
                canonicalMetadataKey = key;
            }
        }

        node = paramNode.get("scope");
        if (node != null) {
            final String scope = node.asText();
            if ("domain".equalsIgnoreCase(scope)) {
                scopeByDomain = true;
            } else if ("host".equalsIgnoreCase(scope)) {
                scopeByDomain = false;
            } else {
                LOG.warn("Unknown value for scope: {}, using host", scope);
            }
        }

        node = paramNode.get("minObservations");
        if (node != null) {
            final int value = node.asInt(DEFAULT_MIN_OBSERVATIONS);
            if (value < 1) {
                LOG.warn("Ignoring invalid value for minObservations: {}", value);
            } else {
                minObservations = value;
            }
        }

        node = paramNode.get("confidenceThreshold");
        if (node != null) {
            final double value = node.asDouble(DEFAULT_CONFIDENCE_THRESHOLD);
            if (value <= 0d || value > 1d) {
                LOG.warn("Ignoring invalid value for confidenceThreshold: {}", value);
            } else {
                confidenceThreshold = value;
            }
        }

        node = paramNode.get("maxScopes");
        if (node != null) {
            final int value = node.asInt(DEFAULT_MAX_SCOPES);
            if (value < 1) {
                LOG.warn("Ignoring invalid value for maxScopes: {}", value);
            } else {
                maxScopes = value;
            }
        }

        node = paramNode.get("maxParams");
        if (node != null) {
            final int value = node.asInt(DEFAULT_MAX_PARAMS);
            if (value < 1) {
                LOG.warn("Ignoring invalid value for maxParams: {}", value);
            } else {
                maxParams = value;
            }
        }

        stats = buildCache(maxScopes);
    }

    @Override
    public @Nullable String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {
        learn(sourceUrl, sourceMetadata);
        return removeIrrelevantParams(urlToFilter);
    }

    /**
     * Compares the URL of the page being parsed with its canonical tag and records which of its
     * query parameters the site considers irrelevant.
     */
    private void learn(@Nullable URL sourceUrl, @Nullable Metadata sourceMetadata) {
        if (sourceUrl == null || sourceMetadata == null) {
            return;
        }

        final String canonicalValue = sourceMetadata.getFirstValue(canonicalMetadataKey);
        if (StringUtils.isBlank(canonicalValue)) {
            return;
        }

        // the filter is called once per outlink: a page must only be counted once
        final String sourceForm = sourceUrl.toExternalForm();
        if (sourceForm.equals(lastLearnedSource)) {
            return;
        }
        lastLearnedSource = sourceForm;

        // nothing to learn from a URL without a query string
        final Set<String> sourceParams = parameterNames(sourceUrl.getQuery());
        if (sourceParams.isEmpty()) {
            return;
        }

        final URL canonical;
        try {
            canonical = URLUtil.resolveUrl(sourceUrl, canonicalValue);
        } catch (MalformedURLException e) {
            LOG.debug("Invalid canonical value {} found in {}", canonicalValue, sourceForm);
            return;
        }

        // a canonical pointing at another resource tells us nothing about the parameters
        if (!sameResource(sourceUrl, canonical)) {
            return;
        }

        final String scopeKey = scopeKey(sourceUrl);
        if (scopeKey == null) {
            return;
        }

        final Set<String> canonicalParams = parameterNames(canonical.getQuery());
        final Map<String, ParamStats> scopeStats = stats.get(scopeKey, k -> new HashMap<>());

        for (String param : sourceParams) {
            ParamStats paramStats = scopeStats.get(param);
            if (paramStats == null) {
                if (scopeStats.size() >= maxParams) {
                    LOG.debug(
                            "Not tracking parameter {} for {}: limit of {} reached",
                            param,
                            scopeKey,
                            maxParams);
                    continue;
                }
                paramStats = new ParamStats();
                scopeStats.put(param, paramStats);
            }
            final boolean wasRemovable = isRemovable(paramStats);
            if (canonicalParams.contains(param)) {
                paramStats.kept++;
            } else {
                paramStats.dropped++;
            }
            if (!wasRemovable && isRemovable(paramStats)) {
                LOG.info(
                        "Removing param {} from the URLs of {}: dropped by {} of {} canonicals",
                        param,
                        scopeKey,
                        paramStats.dropped,
                        paramStats.total());
            }
        }
    }

    /** Removes from the URL the parameters which have been found to be irrelevant for its site. */
    private String removeIrrelevantParams(@NotNull String urlToFilter) {
        final URL url;
        try {
            url = URLUtil.toURL(urlToFilter);
        } catch (MalformedURLException e) {
            // leave it to the filters in charge of the validity of the URLs
            return urlToFilter;
        }

        final String query = url.getQuery();
        if (StringUtils.isEmpty(query)) {
            return urlToFilter;
        }

        final String scopeKey = scopeKey(url);
        if (scopeKey == null) {
            return urlToFilter;
        }

        final Map<String, ParamStats> scopeStats = stats.getIfPresent(scopeKey);
        if (scopeStats == null) {
            return urlToFilter;
        }

        final StringBuilder newQuery = new StringBuilder(query.length());
        boolean removedSomething = false;
        // the parameters are kept verbatim so that their encoding is left untouched
        for (String param : query.split("&", -1)) {
            final ParamStats paramStats = scopeStats.get(parameterName(param));
            if (paramStats != null && isRemovable(paramStats)) {
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
        normalized.append(url.getProtocol()).append(':');
        final String authority = url.getAuthority();
        if (StringUtils.isNotEmpty(authority)) {
            normalized.append("//").append(authority);
        }
        normalized.append(url.getPath());
        if (newQuery.length() > 0) {
            normalized.append('?').append(newQuery);
        }
        final String ref = url.getRef();
        if (ref != null) {
            normalized.append('#').append(ref);
        }
        return normalized.toString();
    }

    private boolean isRemovable(ParamStats paramStats) {
        final int total = paramStats.total();
        return total >= minObservations
                && (double) paramStats.dropped / total >= confidenceThreshold;
    }

    /** Key under which the evidence is gathered, i.e. the host or the domain of the URL. */
    private @Nullable String scopeKey(URL url) {
        final String host = url.getHost();
        if (StringUtils.isEmpty(host)) {
            return null;
        }
        final String lowerCasedHost = host.toLowerCase(Locale.ROOT);
        if (!scopeByDomain) {
            return lowerCasedHost;
        }
        final String domain = PaidLevelDomain.getPLD(lowerCasedHost);
        return domain == null ? lowerCasedHost : domain;
    }

    /** Whether both URLs differ by their query string only. */
    private static boolean sameResource(URL source, URL canonical) {
        final String sourceHost = source.getHost();
        final String canonicalHost = canonical.getHost();
        if (StringUtils.isEmpty(sourceHost) || !sourceHost.equalsIgnoreCase(canonicalHost)) {
            return false;
        }
        if (!source.getProtocol().equalsIgnoreCase(canonical.getProtocol())) {
            return false;
        }
        if (port(source) != port(canonical)) {
            return false;
        }
        return source.getPath().equals(canonical.getPath());
    }

    private static int port(URL url) {
        return url.getPort() == -1 ? url.getDefaultPort() : url.getPort();
    }

    /** Names of the parameters found in a query string, in their decoded form. */
    private static Set<String> parameterNames(@Nullable String query) {
        if (StringUtils.isEmpty(query)) {
            return Collections.emptySet();
        }
        final Set<String> names = new HashSet<>();
        for (String param : query.split("&")) {
            if (!param.isEmpty()) {
                names.add(parameterName(param));
            }
        }
        return names;
    }

    /** Name of a single <code>name=value</code> pair, in its decoded form. */
    private static String parameterName(String param) {
        final int equals = param.indexOf('=');
        final String name = equals == -1 ? param : param.substring(0, equals);
        try {
            return URLDecoder.decode(name, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            // malformed percent encoding: compare the names as they are
            return name;
        }
    }

    /** Number of times a given parameter was dropped or kept by a canonical tag. */
    private static final class ParamStats {

        private int dropped;

        private int kept;

        private int total() {
            return dropped + kept;
        }
    }
}
