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

package org.apache.stormcrawler.filtering.host;

import com.fasterxml.jackson.databind.JsonNode;
import crawlercommons.domains.PaidLevelDomain;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Filters URL based on the hostname.
 *
 * <p>This filter has 2 modes:
 *
 * <ul>
 *   <li>if <code>ignoreOutsideHost</code> is <code>true</code>, all URLs with a host different from
 *       the host of the source URL are filtered out
 *   <li>if <code>ignoreOutsideDomain</code> is <code>true</code>, all URLs with a domain different
 *       from the source's domain are filtered out
 * </ul>
 */
public class HostURLFilter extends URLFilter {

    private boolean ignoreOutsideHost;
    private boolean ignoreOutsideDomain;

    private URL previousSourceUrl;
    private String previousSourceHost;
    private String previousSourceDomain;

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        JsonNode filterByHostNode = filterParams.get("ignoreOutsideHost");
        if (filterByHostNode == null) {
            ignoreOutsideHost = false;
        } else {
            ignoreOutsideHost = filterByHostNode.asBoolean(false);
        }

        // ignoreOutsideDomain is not necessary if we require the host to be
        // always the same
        if (!ignoreOutsideHost) {
            JsonNode filterByDomainNode = filterParams.get("ignoreOutsideDomain");
            if (filterByHostNode == null) {
                ignoreOutsideDomain = false;
            } else {
                ignoreOutsideDomain = filterByDomainNode.asBoolean(false);
            }
        } else {
            ignoreOutsideDomain = false;
        }
    }

    @Override
    public @Nullable String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {
        if (sourceUrl == null || (!ignoreOutsideHost && !ignoreOutsideDomain)) {
            return urlToFilter;
        }

        URL url;
        try {
            url = new URL(urlToFilter);
        } catch (MalformedURLException e1) {
            return null;
        }

        String fromHost;
        String fromDomain = null;
        // Using identity comparison because URL.equals performs poorly
        if (sourceUrl == previousSourceUrl) {
            fromHost = previousSourceHost;
            if (ignoreOutsideDomain) {
                fromDomain = previousSourceDomain;
            }
        } else {
            fromHost = sourceUrl.getHost();
            if (ignoreOutsideDomain) {
                fromDomain = PaidLevelDomain.getPLD(fromHost);
            }
            previousSourceHost = fromHost;
            previousSourceDomain = fromDomain;
            previousSourceUrl = sourceUrl;
        }

        // resolve the hosts
        String toHost = url.getHost();

        if (ignoreOutsideHost) {
            if (toHost == null || !toHost.equalsIgnoreCase(fromHost)) {
                return null;
            }
        }

        if (ignoreOutsideDomain) {
            String toDomain = PaidLevelDomain.getPLD(toHost);
            if (toDomain == null || !toDomain.equals(fromDomain)) {
                return null;
            }
        }

        return urlToFilter;
    }
}
