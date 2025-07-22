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
package org.apache.stormcrawler.jsoup;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilters;
import org.apache.stormcrawler.parse.Outlink;
import org.apache.stormcrawler.parse.ParseData;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.stormcrawler.util.MetadataTransfer;
import org.apache.stormcrawler.util.URLUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ParseFilter to extract additional links with Xpath can be configured with e.g.
 *
 * <pre>{@code
 * {
 *   "class": "org.apache.stormcrawler.parse.filter.LinkParseFilter",
 *   "name": "LinkParseFilter",
 *   "params": {
 *     "pattern": "//IMG/@src",
 *     "pattern2": "//VIDEO/SOURCE/@src"
 *   }
 * }
 *
 * }</pre>
 */
public class LinkParseFilter extends XPathFilter {

    private static final Logger LOG = LoggerFactory.getLogger(LinkParseFilter.class);

    private MetadataTransfer metadataTransfer;

    private URLFilters urlFilters;

    @Override
    public void filter(
            String URL, byte[] content, org.jsoup.nodes.Document doc, ParseResult parse) {

        ParseData parseData = parse.get(URL);
        Metadata metadata = parseData.getMetadata();

        Map<String, Outlink> dedup = new HashMap<>();

        for (Outlink o : parse.getOutlinks()) {
            dedup.put(o.getTargetURL(), o);
        }

        java.net.URL sourceUrl;
        try {
            sourceUrl = new URL(URL);
        } catch (MalformedURLException e1) {
            // we would have known by now as previous components check whether
            // the URL is valid
            LOG.error("MalformedURLException on {}", URL);
            return;
        }

        // applies the XPATH expression in the order in which they are produced
        for (List<LabelledExpression> leList : expressions.values()) {
            for (LabelledExpression le : leList) {
                try {
                    List<String> values = le.evaluate(doc);
                    if (values == null || values.isEmpty()) {
                        continue;
                    }
                    for (String target : values) {
                        // resolve URL
                        target = URLUtil.resolveURL(sourceUrl, target).toExternalForm();

                        // apply filtering
                        target = urlFilters.filter(sourceUrl, metadata, target);
                        if (target == null) {
                            continue;
                        }

                        // check whether we already have this link
                        if (dedup.containsKey(target)) {
                            continue;
                        }

                        // create outlink
                        Outlink ol = new Outlink(target);

                        // get the metadata for the outlink from the parent one
                        Metadata metadataOL =
                                metadataTransfer.getMetaForOutlink(target, URL, metadata);

                        ol.setMetadata(metadataOL);
                        dedup.put(ol.getTargetURL(), ol);
                    }
                } catch (Exception e) {
                    LOG.error("Error evaluating {}: {}", le.key, e);
                }
            }
        }

        parse.setOutlinks(new ArrayList<>(dedup.values()));
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        super.configure(stormConf, filterParams);
        this.metadataTransfer = MetadataTransfer.getInstance(stormConf);
        this.urlFilters = URLFilters.fromConf(stormConf);
    }
}
