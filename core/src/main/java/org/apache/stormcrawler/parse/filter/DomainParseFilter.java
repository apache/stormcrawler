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

package org.apache.stormcrawler.parse.filter;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.stormcrawler.util.URLPartitioner;
import org.jetbrains.annotations.NotNull;
import org.w3c.dom.DocumentFragment;

/** Adds domain (or host) to metadata - can be used later on for indexing. */
public class DomainParseFilter extends ParseFilter {

    private URLPartitioner partitioner;

    private String mdKey = "domain";

    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        JsonNode node = filterParams.get("key");
        if (node != null && node.isTextual()) {
            mdKey = node.asText("domain");
        }

        String partitionMode = Constants.PARTITION_MODE_DOMAIN;

        node = filterParams.get("byHost");
        if (node != null && node.asBoolean()) {
            partitionMode = Constants.PARTITION_MODE_HOST;
        }

        partitioner = new URLPartitioner();
        Map<String, Object> config = new HashMap<>();
        config.put(Constants.PARTITION_MODEParamName, partitionMode);
        partitioner.configure(config);
    }

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {
        Metadata metadata = parse.get(url).getMetadata();
        String value = partitioner.getPartition(url, metadata);
        metadata.setValue(mdKey, value);
    }
}
