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
package org.apache.stormcrawler.filtering.basic;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Simple URL filters : can be used early in the filtering chain */
public class BasicURLFilter extends URLFilter {

    private int maxPathRepetition = 3;
    private int maxLength = -1;

    @Nullable
    public String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {
        if (maxLength > 0 && urlToFilter.length() > maxLength) {
            return null;
        }
        if (maxPathRepetition > 1) {
            urlToFilter = filterPathRepeat(urlToFilter);
        }
        return urlToFilter;
    }

    public final String filterPathRepeat(String urlToFilter) {
        // check whether a path element is repeated N times
        String[] paths = urlToFilter.split("/");

        final int minimumPathLength = 5;
        // e.g. http://www.example.com/path/path will need 5 parts to have a repetition

        if (paths.length < minimumPathLength) return urlToFilter;

        Map<String, Integer> count = new HashMap<>();
        for (String s : paths) {
            if (s.length() == 0) {
                continue;
            }
            Integer c = count.get(s);
            if (c == null) {
                c = 1;
            } else {
                c = c + 1;
                if (c == maxPathRepetition) {
                    return null;
                }
            }
            count.put(s, c);
        }

        return urlToFilter;
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        JsonNode repeat = filterParams.get("maxPathRepetition");
        if (repeat != null) {
            maxPathRepetition = repeat.asInt(3);
        }

        JsonNode length = filterParams.get("maxLength");
        if (length != null) {
            maxLength = length.asInt(-1);
        }
    }
}
