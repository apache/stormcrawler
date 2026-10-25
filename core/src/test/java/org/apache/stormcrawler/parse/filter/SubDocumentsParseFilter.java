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

import java.nio.charset.StandardCharsets;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

public class SubDocumentsParseFilter extends ParseFilter {

    @Override
    public void filter(String url, byte[] content, ParseResult parse) {
        if (content == null) {
            return;
        }

        String html = new String(content, StandardCharsets.UTF_8);
        for (Element link : Jsoup.parse(html, url).select("a[href]")) {
            String href = link.attr("href");
            if (!href.isEmpty()) {
                // create a sub-document for each link; the metadata entry
                // makes sure that it is not skipped as an empty document
                parse.put(href, "isSubDocument", "true");
            }
        }
    }
}
