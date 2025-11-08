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

package org.apache.stormcrawler.filtering.depth;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URL;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilter;
import org.apache.stormcrawler.util.MetadataTransfer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter out URLs whose depth is greater than maxDepth. If its value is set to 0 then no outlinks
 * are followed at all.
 */
public class MaxDepthFilter extends URLFilter {

    private static final Logger LOG = LoggerFactory.getLogger(MaxDepthFilter.class);

    private int maxDepth;

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode paramNode) {
        JsonNode node = paramNode.get("maxDepth");
        if (node != null && node.isInt()) {
            maxDepth = node.intValue();
        } else {
            maxDepth = -1;
            LOG.warn("maxDepth parameter not found");
        }
        LOG.info("maxDepth set to {}", maxDepth);
    }

    @Override
    public @Nullable String filter(
            @Nullable URL pageUrl, @Nullable Metadata sourceMetadata, @NotNull String url) {
        final int depth = getDepth(sourceMetadata, MetadataTransfer.depthKeyName, 0);
        // is there a custom value set for this particular URL?
        final int max = getDepth(sourceMetadata, MetadataTransfer.maxDepthKeyName, maxDepth);
        if (max >= 0 && depth >= max) {
            LOG.debug("filtered out {} - depth {} >= {}", url, depth, maxDepth);
            return null;
        }
        return url;
    }

    private int getDepth(Metadata sourceMetadata, String key, int defaultValue) {
        if (sourceMetadata == null) {
            return defaultValue;
        }
        String depth = sourceMetadata.getFirstValue(key);
        if (StringUtils.isNumeric(depth)) {
            return Integer.parseInt(depth);
        } else {
            return defaultValue;
        }
    }
}
