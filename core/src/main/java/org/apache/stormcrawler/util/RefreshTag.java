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

package org.apache.stormcrawler.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Element;
import org.jsoup.select.Collector;
import org.jsoup.select.Evaluator;
import org.jsoup.select.QueryParser;

// Utility class used to extract refresh tags from HTML pages
public abstract class RefreshTag {

    private static final Matcher MATCHER =
            Pattern.compile("^.*;\\s*URL='?(.+?)'?$", Pattern.CASE_INSENSITIVE).matcher("");

    private static final Evaluator EVALUATOR =
            QueryParser.parse("meta[http-equiv~=(?i)refresh][content]");

    // Returns a normalised value of the content attribute for the refresh tag
    public static String extractRefreshURL(String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }

        // 0;URL=http://www.apollocolors.com/site
        try {
            if (MATCHER.reset(value).matches()) {
                return MATCHER.group(1);
            }
        } catch (Exception e) {
        }

        return null;
    }

    public static String extractRefreshURL(org.jsoup.nodes.Document jsoupDoc) {
        final Element redirElement = Collector.findFirst(EVALUATOR, jsoupDoc);
        if (redirElement != null) {
            return RefreshTag.extractRefreshURL(redirElement.attr("content"));
        }
        return null;
    }
}
