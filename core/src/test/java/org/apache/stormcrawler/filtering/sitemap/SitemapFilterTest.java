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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.bolt.SiteMapParserBolt;
import org.apache.stormcrawler.filtering.URLFilter;
import org.apache.stormcrawler.util.URLUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SitemapFilterTest {

    private static final String OUTLINK = "http://www.example.com/page";

    private URLFilter createFilter(boolean discovery) {
        SitemapFilter filter = new SitemapFilter();
        Map<String, Object> conf = new HashMap<>();
        conf.put("sitemap.discovery", discovery);
        filter.configure(conf, JsonNodeFactory.instance.objectNode(), "SitemapFilter");
        return filter;
    }

    private static Metadata foundSitemap() {
        Metadata metadata = new Metadata();
        metadata.setValue(SiteMapParserBolt.foundSitemapKey, "true");
        return metadata;
    }

    private static URL source() throws MalformedURLException {
        return URLUtil.toURL("http://www.example.com/");
    }

    @Test
    void keptWhenDiscoveryIsOff() throws MalformedURLException {
        URLFilter filter = createFilter(false);
        Assertions.assertEquals(OUTLINK, filter.filter(source(), foundSitemap(), OUTLINK));
    }

    @Test
    void keptWhenTheSiteHasNoSitemap() throws MalformedURLException {
        URLFilter filter = createFilter(true);
        Metadata metadata = new Metadata();
        metadata.setValue(SiteMapParserBolt.foundSitemapKey, "false");
        Assertions.assertEquals(OUTLINK, filter.filter(source(), metadata, OUTLINK));
    }

    /** An outlink of a page which is not a sitemap, on a site which declares sitemaps. */
    @Test
    void outlinkOfNonSitemapPageIsDropped() throws MalformedURLException {
        URLFilter filter = createFilter(true);
        Assertions.assertNull(filter.filter(source(), foundSitemap(), OUTLINK));
    }

    @Test
    void outlinkOfSitemapIsKept() throws MalformedURLException {
        URLFilter filter = createFilter(true);
        Metadata metadata = foundSitemap();
        metadata.setValue(SiteMapParserBolt.isSitemapKey, "true");
        Assertions.assertEquals(OUTLINK, filter.filter(source(), metadata, OUTLINK));
    }

    /** sitemap.discovery in the metadata overrides the configuration. */
    @Test
    void discoveryTurnedOffInMetadataKeepsTheOutlink() throws MalformedURLException {
        URLFilter filter = createFilter(true);
        Metadata metadata = foundSitemap();
        metadata.setValue("sitemap.discovery", "false");
        Assertions.assertEquals(OUTLINK, filter.filter(source(), metadata, OUTLINK));
    }
}
