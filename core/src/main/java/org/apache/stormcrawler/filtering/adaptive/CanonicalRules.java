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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import crawlercommons.domains.PaidLevelDomain;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.URLUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evidence gathered from the canonical tags of a site about which of its query parameters can be
 * removed without changing the content.
 *
 * <p>Fed by {@link org.apache.stormcrawler.parse.filter.CanonicalParamLearner} and read by {@link
 * AdaptiveURLNormalizer}: since these live in different components of the same bolt, they share an
 * instance obtained per JVM and per name with {@link #getInstance(Map, String)}. Configured from the
 * Storm configuration, see the <code>adaptive.normalizer.*</code> options, so that both sides cannot
 * disagree. Safe to use from several threads.
 */
public class CanonicalRules {

    private static final Logger LOG = LoggerFactory.getLogger(CanonicalRules.class);

    public static final String CANONICAL_KEY_PARAM = "adaptive.normalizer.canonical.key";

    public static final String SCOPE_PARAM = "adaptive.normalizer.scope";

    public static final String MIN_OBSERVATIONS_PARAM = "adaptive.normalizer.min.observations";

    public static final String MIN_DISTINCT_PATHS_PARAM = "adaptive.normalizer.min.distinct.paths";

    public static final String CONFIDENCE_PARAM = "adaptive.normalizer.confidence";

    public static final String MAX_SCOPES_PARAM = "adaptive.normalizer.max.scopes";

    public static final String MAX_PARAMS_PARAM = "adaptive.normalizer.max.params";

    public static final String MAX_CACHED_SOURCES_PARAM =
            "adaptive.normalizer.max.cached.sources";

    public static final String PROTECTED_PARAMS_PARAM = "adaptive.normalizer.protected.params";

    /**
     * Never removed, however consistently the canonical tags drop them: a listing serving <code>
     * /list?page=2..N</code> with a canonical of <code>/list</code> would otherwise have its
     * paginated content normalised away and never fetched.
     */
    private static final List<String> DEFAULT_PROTECTED_PARAMS =
            Arrays.asList(
                    "page", "p", "pg", "paged", "offset", "start", "from", "limit", "per_page",
                    "q", "query", "s", "search", "keyword", "keywords", "sort", "order", "dir",
                    "lang", "language", "hl", "locale", "id", "category", "cat", "tag", "year",
                    "month", "day", "view", "format", "type");

    private static final ConcurrentHashMap<String, CanonicalRules> INSTANCES =
            new ConcurrentHashMap<>();

    /** Instance registered under that name for this JVM. The configuration of the first caller wins. */
    public static CanonicalRules getInstance(
            @NotNull Map<String, Object> stormConf, @NotNull String name) {
        return INSTANCES.computeIfAbsent(name, n -> new CanonicalRules(stormConf));
    }

    private final String canonicalKey;

    private final boolean scopeByDomain;

    private final int minObservations;

    private final int minDistinctPaths;

    private final double confidence;

    private final int maxParams;

    private final Set<String> protectedParams;

    /** Evidence per host or domain. */
    private final Cache<String, ConcurrentHashMap<String, ParamStats>> scopes;

    /** Pages already learnt from, so that each counts as a single observation. */
    private final Cache<String, Boolean> knownSources;

    CanonicalRules(@NotNull Map<String, Object> stormConf) {
        canonicalKey = ConfUtils.getString(stormConf, CANONICAL_KEY_PARAM, "canonical");
        scopeByDomain =
                "domain".equalsIgnoreCase(ConfUtils.getString(stormConf, SCOPE_PARAM, "host"));
        minObservations = Math.max(1, ConfUtils.getInt(stormConf, MIN_OBSERVATIONS_PARAM, 5));
        minDistinctPaths = Math.max(1, ConfUtils.getInt(stormConf, MIN_DISTINCT_PATHS_PARAM, 3));

        final double configuredConfidence = ConfUtils.getFloat(stormConf, CONFIDENCE_PARAM, 0.9f);
        if (configuredConfidence <= 0d || configuredConfidence > 1d) {
            LOG.warn("Ignoring invalid value for {}: {}", CONFIDENCE_PARAM, configuredConfidence);
            confidence = 0.9d;
        } else {
            confidence = configuredConfidence;
        }

        maxParams = Math.max(1, ConfUtils.getInt(stormConf, MAX_PARAMS_PARAM, 100));

        final Set<String> configuredProtected = new HashSet<>();
        if (stormConf.containsKey(PROTECTED_PARAMS_PARAM)) {
            configuredProtected.addAll(
                    ConfUtils.loadListFromConf(PROTECTED_PARAMS_PARAM, stormConf));
        } else {
            configuredProtected.addAll(DEFAULT_PROTECTED_PARAMS);
        }
        protectedParams = Collections.unmodifiableSet(configuredProtected);

        scopes =
                Caffeine.newBuilder()
                        .maximumSize(Math.max(1, ConfUtils.getInt(stormConf, MAX_SCOPES_PARAM, 10_000)))
                        .build();
        knownSources =
                Caffeine.newBuilder()
                        .maximumSize(
                                Math.max(
                                        1,
                                        ConfUtils.getInt(
                                                stormConf, MAX_CACHED_SOURCES_PARAM, 50_000)))
                        .build();
    }

    /** Metadata key holding the value of the canonical tag. */
    public String getCanonicalKey() {
        return canonicalKey;
    }

    /** Number of hosts or domains tracked. Pending evictions are performed first. */
    public long getTrackedScopes() {
        scopes.cleanUp();
        return scopes.estimatedSize();
    }

    /**
     * Records what the canonical tag of a page says about its query parameters: the ones it dropped
     * are evidence that they do not affect the content, the ones it kept are evidence of the
     * opposite.
     *
     * @param sourceUrl the URL of the page which was parsed
     * @param canonicalValue the value of its canonical tag, absolute or relative
     */
    public void learn(@Nullable URL sourceUrl, @Nullable String canonicalValue) {
        if (sourceUrl == null || StringUtils.isBlank(canonicalValue)) {
            return;
        }

        // checked before the cache of known sources so that the many URLs without a
        // query string, which teach us nothing, do not take up room in it
        final Set<String> sourceParams = parameterNames(sourceUrl.getQuery());
        if (sourceParams.isEmpty()) {
            return;
        }

        // a page is a single observation, whether it has one outlink or a thousand
        final String sourceForm = sourceUrl.toExternalForm();
        if (knownSources.asMap().putIfAbsent(sourceForm, Boolean.TRUE) != null) {
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
        final ConcurrentHashMap<String, ParamStats> scopeStats =
                scopes.get(scopeKey, k -> new ConcurrentHashMap<>());
        final String sourcePath = path(sourceUrl);

        for (String param : sourceParams) {
            if (protectedParams.contains(param)) {
                continue;
            }
            final ParamStats stats = statsFor(scopeStats, param, scopeKey);
            if (stats == null) {
                continue;
            }
            if (canonicalParams.contains(param)) {
                stats.kept.incrementAndGet();
            } else {
                stats.dropped.incrementAndGet();
                if (stats.droppedPaths.size() < minDistinctPaths) {
                    stats.droppedPaths.add(sourcePath);
                }
            }
            promoteIfEstablished(scopeKey, param, stats);
        }
    }

    /** Whether the parameter has been established as removable for that host or domain. */
    public boolean isRemovable(@Nullable String scopeKey, @NotNull String param) {
        if (scopeKey == null) {
            return false;
        }
        final ConcurrentHashMap<String, ParamStats> scopeStats = scopes.getIfPresent(scopeKey);
        if (scopeStats == null) {
            return false;
        }
        final ParamStats stats = scopeStats.get(param);
        return stats != null && stats.established;
    }

    /** Key under which the evidence of a URL is gathered, i.e. its host or its domain. */
    public @Nullable String scopeKey(@NotNull URL url) {
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

    /**
     * Statistics of a parameter, created if there is room. Room is made by discarding the weakest
     * entry, so that a site using per-page tokens as parameter names cannot fill the slots of a host
     * for good.
     */
    private @Nullable ParamStats statsFor(
            ConcurrentHashMap<String, ParamStats> scopeStats, String param, String scopeKey) {
        ParamStats stats = scopeStats.get(param);
        if (stats != null) {
            return stats;
        }
        synchronized (scopeStats) {
            stats = scopeStats.get(param);
            if (stats != null) {
                return stats;
            }
            if (scopeStats.size() >= maxParams && !discardWeakest(scopeStats)) {
                LOG.debug("Not tracking parameter {} for {}: no room left", param, scopeKey);
                return null;
            }
            stats = new ParamStats();
            scopeStats.put(param, stats);
            return stats;
        }
    }

    /** Discards the parameter with the least evidence, established ones excepted. */
    private boolean discardWeakest(ConcurrentHashMap<String, ParamStats> scopeStats) {
        String weakest = null;
        int fewest = Integer.MAX_VALUE;
        for (Map.Entry<String, ParamStats> entry : scopeStats.entrySet()) {
            final ParamStats stats = entry.getValue();
            if (stats.established) {
                continue;
            }
            final int total = stats.total();
            if (total < fewest) {
                fewest = total;
                weakest = entry.getKey();
            }
        }
        if (weakest == null) {
            return false;
        }
        scopeStats.remove(weakest);
        return true;
    }

    /**
     * Promotes a parameter to removable once the evidence is sufficient. Promotions are final: a
     * rule which came and went would normalise the same URL differently over time.
     */
    private void promoteIfEstablished(String scopeKey, String param, ParamStats stats) {
        if (stats.established) {
            return;
        }
        final int dropped = stats.dropped.get();
        final int total = dropped + stats.kept.get();
        if (total < minObservations
                || (double) dropped / total < confidence
                || stats.droppedPaths.size() < minDistinctPaths) {
            return;
        }
        stats.established = true;
        LOG.info(
                "Removing param {} from the URLs of {}: dropped by {} of {} pages, {} paths",
                param,
                scopeKey,
                dropped,
                total,
                stats.droppedPaths.size());
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
        return path(source).equals(path(canonical));
    }

    private static int port(URL url) {
        return url.getPort() == -1 ? url.getDefaultPort() : url.getPort();
    }

    /** Path of a URL, never null: opaque URLs such as <code>mailto:</code> have none. */
    private static String path(URL url) {
        final String path = url.getPath();
        return path == null ? "" : path;
    }

    /** Names of the parameters found in a query string, in their decoded form. */
    static Set<String> parameterNames(@Nullable String query) {
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
    static String parameterName(String param) {
        final int equals = param.indexOf('=');
        final String name = equals == -1 ? param : param.substring(0, equals);
        try {
            return URLDecoder.decode(name, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            // malformed percent encoding: compare the names as they are
            return name;
        }
    }

    /** What the canonical tags said about a given parameter of a given host or domain. */
    private static final class ParamStats {

        private final AtomicInteger dropped = new AtomicInteger();

        private final AtomicInteger kept = new AtomicInteger();

        /** Distinct paths whose canonical dropped the parameter, capped to what is needed. */
        private final Set<String> droppedPaths = ConcurrentHashMap.newKeySet();

        private volatile boolean established;

        private int total() {
            return dropped.get() + kept.get();
        }
    }
}
