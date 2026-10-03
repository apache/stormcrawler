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
 * AdaptiveURLNormalizer}: since these live in different components of the same bolt, they share the
 * single instance of this JVM, obtained with {@link #getInstance(Map)} and handed back with {@link
 * #release()}. Configured from the Storm configuration, see the <code>adaptive.normalizer.*</code>
 * options, so that both sides cannot disagree. Safe to use from several threads.
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

    public static final String MAX_CACHED_SOURCES_PARAM = "adaptive.normalizer.max.cached.sources";

    public static final String PROTECTED_PARAMS_PARAM = "adaptive.normalizer.protected.params";

    /**
     * Never removed, however consistently the canonical tags drop them, matched regardless of case:
     * a listing serving <code>/list?page=2..N</code> with a canonical of <code>/list</code> would
     * otherwise have its paginated content normalised away and never fetched.
     */
    private static final List<String> DEFAULT_PROTECTED_PARAMS =
            Arrays.asList(
                    "page",
                    "p",
                    "pg",
                    "pgno",
                    "pg_no",
                    "pageno",
                    "page_no",
                    "pagenum",
                    "page_num",
                    "pagenumber",
                    "page_number",
                    "pageindex",
                    "page_index",
                    "paged",
                    "pagina",
                    "seite",
                    "offset",
                    "start",
                    "from",
                    "limit",
                    "per_page",
                    "perpage",
                    "q",
                    "query",
                    "s",
                    "search",
                    "keyword",
                    "keywords",
                    "sort",
                    "order",
                    "orderby",
                    "order_by",
                    "dir",
                    "lang",
                    "language",
                    "hl",
                    "locale",
                    "id",
                    "category",
                    "cat",
                    "tag",
                    "year",
                    "month",
                    "day",
                    "view",
                    "format",
                    "type");

    /** Guarded by the class lock, as is the number of components using it. */
    private static CanonicalRules instance;

    private static int users;

    /**
     * The instance shared by the components of this JVM, created from the Storm configuration by
     * the first of them. Every caller must {@link #release()} it once done, so that the next
     * topology of a long-lived JVM starts afresh. A caller whose settings differ from the existing
     * instance's gets that instance nonetheless, with a warning.
     */
    public static synchronized CanonicalRules getInstance(@NotNull Map<String, Object> stormConf) {
        final Settings settings = Settings.from(stormConf);
        if (instance == null) {
            instance = new CanonicalRules(settings);
        } else if (!instance.settings.equals(settings)) {
            LOG.warn("Already configured differently in this JVM, keeping the first settings");
        }
        users++;
        return instance;
    }

    /** Hands the instance back; once no component uses it any more it is discarded. */
    public void release() {
        synchronized (CanonicalRules.class) {
            if (instance != this) {
                return;
            }
            if (--users <= 0) {
                instance = null;
                users = 0;
            }
        }
    }

    /** What is read from the Storm configuration, with the defaults applied. */
    private record Settings(
            String canonicalKey,
            boolean scopeByDomain,
            int minObservations,
            int minDistinctPaths,
            double confidence,
            int maxScopes,
            int maxParams,
            int maxCachedSources,
            Set<String> protectedParams) {

        static Settings from(Map<String, Object> stormConf) {
            double confidence = ConfUtils.getFloat(stormConf, CONFIDENCE_PARAM, 0.9f);
            if (confidence <= 0d || confidence > 1d) {
                LOG.warn("Ignoring invalid value for {}: {}", CONFIDENCE_PARAM, confidence);
                confidence = 0.9d;
            }

            final List<String> configuredProtected =
                    stormConf.containsKey(PROTECTED_PARAMS_PARAM)
                            ? ConfUtils.loadListFromConf(PROTECTED_PARAMS_PARAM, stormConf)
                            : DEFAULT_PROTECTED_PARAMS;
            final Set<String> protectedParams = new HashSet<>();
            for (String param : configuredProtected) {
                protectedParams.add(param.toLowerCase(Locale.ROOT));
            }

            return new Settings(
                    ConfUtils.getString(stormConf, CANONICAL_KEY_PARAM, "canonical"),
                    "domain".equalsIgnoreCase(ConfUtils.getString(stormConf, SCOPE_PARAM, "host")),
                    Math.max(1, ConfUtils.getInt(stormConf, MIN_OBSERVATIONS_PARAM, 5)),
                    Math.max(1, ConfUtils.getInt(stormConf, MIN_DISTINCT_PATHS_PARAM, 3)),
                    confidence,
                    Math.max(1, ConfUtils.getInt(stormConf, MAX_SCOPES_PARAM, 10_000)),
                    Math.max(1, ConfUtils.getInt(stormConf, MAX_PARAMS_PARAM, 20)),
                    Math.max(1, ConfUtils.getInt(stormConf, MAX_CACHED_SOURCES_PARAM, 50_000)),
                    Collections.unmodifiableSet(protectedParams));
        }
    }

    private final Settings settings;

    /** Evidence per host or domain. */
    private final Cache<String, ConcurrentHashMap<String, ParamStats>> scopes;

    /** Pages already learnt from, so that each counts as a single observation. */
    private final Cache<String, Boolean> knownSources;

    private CanonicalRules(Settings settings) {
        this.settings = settings;
        scopes = Caffeine.newBuilder().maximumSize(settings.maxScopes()).build();
        knownSources = Caffeine.newBuilder().maximumSize(settings.maxCachedSources()).build();
    }

    /** Metadata key holding the value of the canonical tag. */
    public String getCanonicalKey() {
        return settings.canonicalKey();
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
        final int sourcePath = path(sourceUrl).hashCode();

        for (String param : sourceParams) {
            if (settings.protectedParams().contains(param.toLowerCase(Locale.ROOT))) {
                continue;
            }
            final ParamStats stats = statsFor(scopeStats, param, scopeKey);
            if (stats == null) {
                continue;
            }
            if (canonicalParams.contains(param)) {
                stats.recordKept();
            } else {
                stats.recordDropped(sourcePath);
            }
            promoteIfEstablished(scopeStats, scopeKey, param, stats);
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
        if (!settings.scopeByDomain()) {
            return lowerCasedHost;
        }
        final String domain = PaidLevelDomain.getPLD(lowerCasedHost);
        return domain == null ? lowerCasedHost : domain;
    }

    /**
     * Statistics of a parameter, created if there is room. Room is made by discarding the weakest
     * entry, so that a site using per-page tokens as parameter names cannot fill the slots of a
     * host for good.
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
            if (scopeStats.size() >= settings.maxParams() && !discardWeakest(scopeStats)) {
                LOG.debug("Not tracking parameter {} for {}: no room left", param, scopeKey);
                return null;
            }
            stats = new ParamStats(settings.minDistinctPaths());
            scopeStats.put(param, stats);
            return stats;
        }
    }

    /**
     * Discards the parameter with the least evidence, established ones excepted. Called with the
     * lock on the map held, which promotions take too: a parameter cannot be promoted and discarded
     * at the same time.
     */
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
    private void promoteIfEstablished(
            ConcurrentHashMap<String, ParamStats> scopeStats,
            String scopeKey,
            String param,
            ParamStats stats) {
        if (stats.established || !stats.isEstablishedBy(settings)) {
            return;
        }
        synchronized (scopeStats) {
            // discarded by another thread in the meantime, or promoted by one
            if (scopeStats.get(param) != stats || stats.established) {
                return;
            }
            stats.established = true;
        }
        LOG.info("Removing param {} from the URLs of {}: {}", param, scopeKey, stats);
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

    /**
     * What the canonical tags said about a given parameter of a given host or domain. Kept small on
     * purpose, there can be up to <code>max.scopes * max.params</code> of them: the counters are
     * guarded by the monitor of the object and the distinct paths are stored as hashes, as many as
     * required to establish a rule. A collision between two paths merely makes the rule harder to
     * establish.
     */
    private static final class ParamStats {

        private int dropped;

        private int kept;

        private final int[] droppedPaths;

        private int distinctPaths;

        /** Written with the lock on the enclosing map held, read without any lock. */
        private volatile boolean established;

        private ParamStats(int pathsRequired) {
            droppedPaths = new int[pathsRequired];
        }

        private synchronized void recordKept() {
            kept++;
        }

        private synchronized void recordDropped(int pathHash) {
            dropped++;
            if (distinctPaths == droppedPaths.length) {
                return;
            }
            for (int i = 0; i < distinctPaths; i++) {
                if (droppedPaths[i] == pathHash) {
                    return;
                }
            }
            droppedPaths[distinctPaths++] = pathHash;
        }

        private synchronized int total() {
            return dropped + kept;
        }

        private synchronized boolean isEstablishedBy(Settings settings) {
            final int total = dropped + kept;
            return total >= settings.minObservations()
                    && (double) dropped / total >= settings.confidence()
                    && distinctPaths >= settings.minDistinctPaths();
        }

        @Override
        public synchronized String toString() {
            return "dropped by "
                    + dropped
                    + " of "
                    + (dropped + kept)
                    + " pages, "
                    + distinctPaths
                    + " paths";
        }
    }
}
