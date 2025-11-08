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

package org.apache.stormcrawler.filtering.sitemap;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URL;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.bolt.SiteMapParserBolt;
import org.apache.stormcrawler.filtering.URLFilter;
import org.apache.stormcrawler.util.ConfUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * URLFilter which discards URLs discovered in a page which is not a sitemap when sitemaps have been
 * found for that site. This allows to restrict the crawl to pages found in the sitemaps but won't
 * affect sites which do not have sitemaps.
 *
 * <pre>
 *  {
 *    "class": "org.apache.stormcrawler.filtering.sitemap.SitemapFilter",
 *    "name": "SitemapFilter"
 *  }
 * </pre>
 *
 * <p>Will be replaced by <a href=
 * "https://github.com/apache/stormcrawler/issues/711">MetadataFilter to filter based on multiple
 * key values</a>
 *
 * @since 1.14
 */
public class SitemapFilter extends URLFilter {

    private static final String SITEMAP_DISCOVERY_PARAM_KEY = "sitemap.discovery";

    private boolean sitemapsAutoDiscovery = false;

    @Override
    public void configure(
            @NotNull Map<String, Object> stormConf,
            @NotNull JsonNode filtersConf,
            @NotNull String name) {
        super.configure(stormConf, filtersConf);
        sitemapsAutoDiscovery = ConfUtils.getBoolean(stormConf, SITEMAP_DISCOVERY_PARAM_KEY, false);
    }

    @Override
    public @Nullable String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {
        if (sourceMetadata == null) {
            return urlToFilter;
        }
        boolean smautodisco = false;
        // check in the metadata if discovery setting has been
        // overridden
        String localSitemapDiscoveryVal = sourceMetadata.getFirstValue(SITEMAP_DISCOVERY_PARAM_KEY);

        if ("true".equalsIgnoreCase(localSitemapDiscoveryVal)) {
            smautodisco = true;
        } else if ("false".equalsIgnoreCase(localSitemapDiscoveryVal)) {
            smautodisco = false;
        } else {
            smautodisco = sitemapsAutoDiscovery;
        }
        if (!smautodisco) {
            return urlToFilter;
        } else if (!Boolean.parseBoolean(
                        sourceMetadata.getFirstValue(SiteMapParserBolt.isSitemapKey))
                && Boolean.parseBoolean(
                        sourceMetadata.getFirstValue(SiteMapParserBolt.foundSitemapKey))) {
            return null;
        }
        return urlToFilter;
    }
}
