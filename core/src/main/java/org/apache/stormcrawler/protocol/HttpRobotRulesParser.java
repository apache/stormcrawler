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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.URLUtil;

/**
 * This class is used for parsing robots for urls belonging to HTTP protocol. It extends the generic
 * {@link RobotRulesParser} class and contains Http protocol specific implementation for obtaining
 * the robots file.
 */
public class HttpRobotRulesParser extends RobotRulesParser {

    protected boolean allowForbidden = true;

    protected boolean allow5xx = false;

    protected Metadata fetchRobotsMd;

    private static final int MAX_NUM_REDIRECTS = 5;

    HttpRobotRulesParser() {}

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
    }

    /** Compose unique key to store and access robot rules in cache for given URL */
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
     * Returns the robots rules from the cache or empty rules if not found
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
        Set<String> redirectCacheKeys = new HashSet<>();

        URL robotsUrl = null, redir = null;

        LOG.debug("Cache miss {} for {}", cacheKey, url);
        List<Integer> bytesFetched = new LinkedList<>();
        try {
            robotsUrl = URLUtil.resolveURL(url, "/robots.txt");
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
                String redirection = response.getMetadata().getFirstValue(HttpHeaders.LOCATION);
                LOG.debug("Redirected from {} to {}", redir, redirection);
                if (StringUtils.isNotBlank(redirection)) {
                    redir = URLUtil.resolveURL(redir, redirection);
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
                                "Robots for {} redirected to {} (not cached for target host because not at root)",
                                url,
                                redir);
                    }

                    response = http.getProtocolOutput(redir.toString(), Metadata.empty);
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
                String ct = response.getMetadata().getFirstValue(HttpHeaders.CONTENT_TYPE);
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
        for (String keyredir : redirectCacheKeys) {
            // keyredir isn't null only if the robots.txt file of the target is
            // at the root
            LOG.debug("Caching robots for {} under key {} in cache {}", redir, keyredir, cacheName);
            cacheToUse.put(keyredir, cached);
        }

        RobotRules live = new RobotRules(robotRules);
        live.setContentLengthFetched(Ints.toArray(bytesFetched));
        return live;
    }
}
