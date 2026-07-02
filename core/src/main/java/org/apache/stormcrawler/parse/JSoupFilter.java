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

package org.apache.stormcrawler.parse;

import org.apache.stormcrawler.util.Configurable;
import org.jetbrains.annotations.NotNull;

/**
 * Implementations of ParseFilter are responsible for extracting custom data from the crawled
 * content. They are used exclusively by {@link org.apache.stormcrawler.bolt.JSoupParserBolt}.
 */
public interface JSoupFilter extends Configurable {
    /**
     * Called when parsing a specific page.
     *
     * @param url the URL of the page being parsed
     * @param content the content being parsed
     * @param doc document produced by JSoup's parsingF
     * @param parse the metadata to be updated with the resulting of the parsing
     */
    void filter(
            @NotNull String url,
            byte[] content,
            @NotNull org.jsoup.nodes.Document doc,
            @NotNull ParseResult parse);
}
