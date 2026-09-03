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

package org.apache.stormcrawler.protocol;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.primitives.Ints;
import crawlercommons.robots.BaseRobotRules;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.HttpHeaderResolver;
import org.apache.stormcrawler.util.URLUtil;

/**
 * This class is used for parsing robots for urls belonging to HTTP protocol. It extends the generic
 * {@link RobotRulesParser} class and contains Http protocol specific implementation for obtaining
 * the robots file.
 */
public class HttpRobotRulesParser extends RobotRulesParser {

    protected boolean allowForbidden = true;

    protected boolean allow5xx = false;

    protected boolean allowCrossOriginRedirects = false;

    protected boolean allowRefusedRedirect = false;

    protected Metadata fetchRobotsMd;

    private static final int MAX_NUM_REDIRECTS = 5;

    public HttpRobotRulesParser() {}

    public HttpRobotRulesParser(Config conf) {
        setConf(conf);
    }

    @Override
    public void setConf(Config conf) {
        super.setConf(conf);
        allowForbidden = ConfUtils.getBoolean(conf, "http.robots.403.allow", true);
        fetchRobotsMd = new Metadata();
        /* http.content.limit for fetching the robots.txt */
        int robotsTxtContentLimit = ConfUtils.getInt(conf, "http.robots.content.limit", -1);
        fetchRobotsMd.addValue("http.content.limit", Integer.toString(robotsTxtContentLimit));
        allow5xx = ConfUtils.getBoolean(conf, "http.robots.5xx.allow", false);
        allowCrossOriginRedirects =
                ConfUtils.getBoolean(conf, "http.robots.redirect.crossorigin.allow", false);
        allowRefusedRedirect =
                ConfUtils.getBoolean(conf, "http.robots.redirect.refused.allow", false);
        if (allowCrossOriginRedirects) {
            logForwardedRequestHeaders(conf);
        }
    }

    /**
     * Logs which of the configured request headers are sent to the target of a robots.txt redirect
     * to another host. The headers are configured per protocol instance and the redirect is
     * followed by re-fetching the target through the same instance, so every hop of the chain is
     * requested with them.
     */
    private static void logForwardedRequestHeaders(Config conf) {
        List<String> sources = new ArrayList<>();
        if (StringUtils.isNotBlank(ConfUtils.getString(conf, "http.basicauth.user", null))) {
            sources.add("http.basicauth.user");
        }
        if (!ConfUtils.loadListFromConf("http.custom.headers", conf).isEmpty()) {
            sources.add("http.custom.headers");
        }
        if (!sources.isEmpty()) {
            LOG.warn(
                    "http.robots.redirect.crossorigin.allow is set: a robots.txt redirect is fetched "
                            + "from the host named in the Location header, and the request headers "
                            + "configured by {} are sent to it as to any other host",
                    String.join(" and ", sources));
        }
    }

    /** Compose unique key to store and access robot rules in cache for given URL. */
    protected static String getCacheKey(URL url) {
        String protocol = url.getProtocol().toLowerCase(Locale.ROOT);
        String host = url.getHost().toLowerCase(Locale.ROOT);

        int port = url.getPort();
        if (port == -1) {
            port = url.getDefaultPort();
        }
        /*
         * Robot rules apply only to host, protocol, and port where robots.txt
         * is hosted (cf. NUTCH-1752). Consequently
         */
        return protocol + ":" + host + ":" + port;
    }

    /**
     * Checks whether a redirect while fetching a robots.txt is followed. The target must use the
     * http or https scheme and, unless {@code http.robots.redirect.crossorigin.allow} is set,
     * either share scheme, host and port with the URL it was reached from, or be the same host and
     * port reached over https instead of http.
     *
     * @param from URL which returned the redirect
     * @param target resolved value of the Location header
     * @return true if the target may be fetched
     */
    protected boolean followRedirect(URL from, URL target) {
        String scheme = target.getProtocol().toLowerCase(Locale.ROOT);
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
            return false;
        }
        return allowCrossOriginRedirects
                || getCacheKey(from).equals(getCacheKey(target))
                || isSchemeUpgrade(from, target);
    }

    /**
     * Checks whether a redirect only replaces http with https while staying on the same host and
     * port, e.g. {@code http://example.com/robots.txt} to {@code https://example.com/robots.txt}.
     * {@link #getCacheKey(URL)} derives the default port from the scheme, so such a target has a
     * different key although no other host is involved. It is therefore fetched even when redirects
     * to a different origin are not followed.
     *
     * <p>Only this direction is exempt. The reverse, https to http, would send the request headers
     * configured for the fetch, e.g. the Authorization header built from {@code
     * http.basicauth.user}, unencrypted, and would let the rules read over a plain text connection
     * replace the ones the redirect was reached from. A site relying on that redirect needs {@code
     * http.robots.redirect.crossorigin.allow}.
     */
    private static boolean isSchemeUpgrade(URL from, URL target) {
        if (!"http".equals(from.getProtocol().toLowerCase(Locale.ROOT))
                || !"https".equals(target.getProtocol().toLowerCase(Locale.ROOT))) {
            return false;
        }
        if (!from.getHost().equalsIgnoreCase(target.getHost())) {
            return false;
        }
        // either the port is unchanged or both sides use the default port of their scheme
        return from.getPort() == target.getPort() || (isDefaultPort(from) && isDefaultPort(target));
    }

    /** Checks whether a URL uses the default port of its scheme, explicitly or implicitly. */
    private static boolean isDefaultPort(URL url) {
        return url.getPort() == -1 || url.getPort() == url.getDefaultPort();
    }

    /** Describes for logging why the target of a redirect is not fetched. */
    private static String redirectRefusalReason(URL from, URL target) {
        String targetScheme = target.getProtocol().toLowerCase(Locale.ROOT);
        if (!"http".equals(targetScheme) && !"https".equals(targetScheme)) {
            return "on neither http nor https";
        }
        if (!from.getHost().equalsIgnoreCase(target.getHost())) {
            return "on a different host than " + from;
        }
        if (!from.getProtocol().toLowerCase(Locale.ROOT).equals(targetScheme)) {
            return "on a different scheme than " + from;
        }
        return "on a different port than " + from;
    }

    /**
     * Returns the robots rules from the cache or empty rules if not found.
     *
     * @see org.apache.stormcrawler.filtering.robots.RobotsFilter
     */
    public BaseRobotRules getRobotRulesSetFromCache(URL url) {
        String cacheKey = getCacheKey(url);
        BaseRobotRules robotRules = CACHE.getIfPresent(cacheKey);
        if (robotRules != null) {
            return robotRules;
        }
        return EMPTY_RULES;
    }

    /**
     * Get the rules from robots.txt which applies for the given {@code url}. Robot rules are cached
     * for a unique combination of host, protocol, and port. If no rules are found in the cache, a
     * HTTP request is send to fetch {{protocol://host:port/robots.txt}}. The robots.txt is then
     * parsed and the rules are cached to avoid re-fetching and re-parsing it again.
     *
     * @param http The {@link Protocol} object
     * @param url URL robots.txt applies to
     * @return {@link BaseRobotRules} holding the rules from robots.txt
     */
    @Override
    public BaseRobotRules getRobotRulesSet(Protocol http, URL url) {

        String cacheKey = getCacheKey(url);

        // check in the error cache first
        BaseRobotRules robotRules = ERRORCACHE.getIfPresent(cacheKey);
        if (robotRules != null) {
            return robotRules;
        }

        // now try the proper cache
        robotRules = CACHE.getIfPresent(cacheKey);
        if (robotRules != null) {
            return robotRules;
        }

        boolean cacheRule = true;
        boolean redirectRefused = false;
        Set<String> redirectCacheKeys = new HashSet<>();

        URL robotsUrl = null;
        URL redir = null;

        LOG.debug("Cache miss {} for {}", cacheKey, url);
        List<Integer> bytesFetched = new LinkedList<>();
        try {
            robotsUrl = URLUtil.resolveUrl(url, "/robots.txt");
            ProtocolResponse response = http.getProtocolOutput(robotsUrl.toString(), fetchRobotsMd);
            int code = response.getStatusCode();
            bytesFetched.add(response.getContent() != null ? response.getContent().length : 0);

            // According to RFC9309, the crawler should follow at least 5 consecutive redirects
            // to get the robots.txt file.
            int numRedirects = 0;
            // The base URL to resolve relative redirect locations is set initially to the default
            // URL path ("/robots.txt") and updated when redirects were followed.
            redir = robotsUrl;

            while ((code == 301 || code == 302 || code == 303 || code == 307 || code == 308)
                    && numRedirects < MAX_NUM_REDIRECTS) {
                numRedirects++;
                String redirection =
                        HttpHeaderResolver.getFirstValue(
                                response.getMetadata(), HttpHeaders.LOCATION);
                LOG.debug("Redirected from {} to {}", redir, redirection);
                if (StringUtils.isNotBlank(redirection)) {
                    URL target = URLUtil.resolveUrl(redir, redirection);
                    if (!followRedirect(redir, target)) {
                        redirectRefused = true;
                        LOG.warn(
                                "Robots for {} redirected to {} which is not fetched, the target "
                                        + "is {}. As a result {}. Set "
                                        + "http.robots.redirect.crossorigin.allow to true to follow "
                                        + "such redirects, or "
                                        + "http.robots.redirect.refused.allow to change what "
                                        + "happens when one of them is not followed.",
                                url,
                                target,
                                redirectRefusalReason(redir, target),
                                allowRefusedRedirect
                                        ? "the host is crawled without any rules"
                                        : "nothing is crawled on that host");
                        break;
                    }
                    redir = target;
                    if (redir.getPath().equals("/robots.txt") && redir.getQuery() == null) {
                        // only if the path (including the query part) of the redirect target is
                        // `/robots.txt` we can get/put the rules from/to the cache under the host
                        // key of the redirect target
                        String keyredir = getCacheKey(redir);
                        RobotRules cachedRediRobotRules = CACHE.getIfPresent(keyredir);
                        if (cachedRediRobotRules != null) {
                            // cache also for the source host
                            LOG.debug(
                                    "Found robots for {} (redirected) under key {} in cache",
                                    redir,
                                    keyredir);
                            LOG.debug(
                                    "Caching redirected robots from key {} under key {}",
                                    keyredir,
                                    cacheKey);
                            CACHE.put(cacheKey, cachedRediRobotRules);
                            return cachedRediRobotRules;
                        } else {
                            // Remember the target host/authority, we can cache the rules, too.
                            redirectCacheKeys.add(keyredir);
                        }
                    } else {
                        LOG.debug(
                                "Robots for {} redirected to {} "
                                        + "(not cached for target host because not at root)",
                                url,
                                redir);
                    }

                    response = http.getProtocolOutput(redir.toString(), fetchRobotsMd);
                    code = response.getStatusCode();
                    bytesFetched.add(
                            response.getContent() != null ? response.getContent().length : 0);
                } else {
                    LOG.debug("Got redirect response {} for robots {} without location", code, url);
                    break;
                }
            }

            // Parsing found rules according to RFC 9309
            if (code == 200) {
                // Only if the status code 200 is returned, the rules are parsed
                String ct =
                        HttpHeaderResolver.getFirstValue(
                                response.getMetadata(), HttpHeaders.CONTENT_TYPE);
                robotRules = parseRules(url.toString(), response.getContent(), ct, agentNames);
            } else if (code == 403 && !allowForbidden) {
                // If the fetch of the robots.txt file is forbidden, then forbid also the fetch
                // of the other pages within this host
                robotRules = FORBID_ALL_RULES;
            } else if (code == 429) {
                // Handling Too many requests similar to a server error
                // https://support.google.com/webmasters/answer/9679690#robots_details
                cacheRule = false;
                robotRules = FORBID_ALL_RULES;
            } else if (code >= 500 && code <= 599) { // in range between 500 and 599
                // If the fetch of the robots.txt file is not possible due to a server error, then
                // better not crawl the remaining pages within this domain
                cacheRule = false;
                robotRules = FORBID_ALL_RULES;
                if (allow5xx) {
                    robotRules = EMPTY_RULES; // allow all
                }
            } else if (redirectRefused) {
                // the redirect was not followed, so no rules were obtained for this host
                cacheRule = false;
                robotRules = allowRefusedRedirect ? EMPTY_RULES : FORBID_ALL_RULES;
            } else {
                robotRules = EMPTY_RULES; // allow all
            }
        } catch (Throwable t) {
            LOG.info("Couldn't get robots.txt for {} : {}", url, t.toString());
            cacheRule = false;
            robotRules = EMPTY_RULES;
        }

        Cache<String, RobotRules> cacheToUse = CACHE;
        String cacheName = "success";
        if (!cacheRule) {
            cacheToUse = ERRORCACHE;
            cacheName = "error";
        }

        RobotRules cached = new RobotRules(robotRules);

        LOG.debug("Caching robots for {} under key {} in cache {}", url, cacheKey, cacheName);
        cacheToUse.put(cacheKey, cached);

        // cache robot rules for redirections
        // get here only if the target has not been found in the cache
        // a chain ending in a redirect which was not followed produced no rules for the hosts on
        // it, so nothing is stored under their keys
        if (!redirectRefused) {
            for (String keyredir : redirectCacheKeys) {
                // keyredir isn't null only if the robots.txt file of the target is
                // at the root
                LOG.debug(
                        "Caching robots for {} under key {} in cache {}",
                        redir,
                        keyredir,
                        cacheName);
                cacheToUse.put(keyredir, cached);
            }
        }

        RobotRules live = new RobotRules(robotRules);
        live.setContentLengthFetched(Ints.toArray(bytesFetched));
        return live;
    }
}
