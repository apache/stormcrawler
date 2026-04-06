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

package org.apache.stormcrawler.opensearch.parse.filter;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.apache.stormcrawler.opensearch.DelegateRefresher;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.jetbrains.annotations.NotNull;
import org.w3c.dom.DocumentFragment;

/**
 * Wraps a ParseFilter whose resources are in a JSON file that can be stored in OpenSearch. The
 * benefit of doing this is that the resources can be refreshed automatically and modified without
 * having to recompile the jar and restart the topology. The connection to OpenSearch is done via
 * the config and uses a new bolt type 'config'.
 *
 * <p>The configuration of the delegate is done in the parsefilters.json as usual.
 *
 * <pre>
 *  {
 *     "class": "org.apache.stormcrawler.opensearch.parse.filter.JSONResourceWrapper",
 *     "name": "OpenSearchCollectionTagger",
 *     "params": {
 *         "refresh": "60",
 *         "delegate": {
 *             "class": "org.apache.stormcrawler.parse.filter.CollectionTagger",
 *             "params": {
 *                 "file": "collections.json"
 *             }
 *         }
 *     }
 *  }
 * </pre>
 *
 * The resource file can be pushed to OpenSearch with
 *
 * <pre>
 *  curl -XPUT "$OSHOST/config/_create/collections.json" -H 'Content-Type: application/json' -d @src/main/resources/collections.json
 * </pre>
 */
public class JSONResourceWrapper extends ParseFilter {

    private DelegateRefresher<ParseFilter> refresher;

    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        refresher =
                new DelegateRefresher<>(
                        ParseFilter.class,
                        stormConf,
                        filterParams,
                        (delegate, conf, params) -> delegate.configure(conf, params));
    }

    @Override
    public void filter(String URL, byte[] content, DocumentFragment doc, ParseResult parse) {
        refresher.getDelegate().filter(URL, content, doc, parse);
    }

    @Override
    public void cleanup() {
        refresher.cleanup();
    }
}
