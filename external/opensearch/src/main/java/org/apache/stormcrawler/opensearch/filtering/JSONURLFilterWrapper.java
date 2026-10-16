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

package org.apache.stormcrawler.opensearch.filtering;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URL;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilter;
import org.apache.stormcrawler.opensearch.DelegateRefresher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Wraps a URLFilter whose resources are in a JSON file that can be stored in OpenSearch. The
 * benefit of doing this is that the resources can be refreshed automatically and modified without
 * having to recompile the jar and restart the topology. The connection to OpenSearch is done via
 * the config and uses a new bolt type 'config'.
 *
 * <p>The configuration of the delegate is done in the urlfilters.json as usual.
 *
 * <pre>
 *  {
 *     "class": "org.apache.stormcrawler.opensearch.filtering.JSONURLFilterWrapper",
 *     "name": "OSFastURLFilter",
 *     "params": {
 *         "refresh": "60",
 *         "delegate": {
 *             "class": "org.apache.stormcrawler.filtering.regex.FastURLFilter",
 *             "params": {
 *                 "file": "fast.urlfilter.json"
 *             }
 *         }
 *     }
 *  }
 * </pre>
 *
 * The resource file can be pushed to OpenSearch with
 *
 * <pre>
 *  curl -XPUT 'localhost:9200/config/config/fast.urlfilter.json?pretty' -H 'Content-Type: application/json' -d @fast.urlfilter.json
 * </pre>
 */
public class JSONURLFilterWrapper extends URLFilter {

    private DelegateRefresher<URLFilter> refresher;

    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        refresher =
                new DelegateRefresher<>(
                        URLFilter.class, stormConf, filterParams, URLFilter::configure);
    }

    @Override
    public @Nullable String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {
        return refresher.getDelegate().filter(sourceUrl, sourceMetadata, urlToFilter);
    }

    @Override
    public void cleanup() {
        if (refresher != null) {
            refresher.cleanup();
        }
    }
}
