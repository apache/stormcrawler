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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Objects;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilter;
import org.apache.stormcrawler.util.URLUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Filters links to self. * */
public class SelfURLFilter extends URLFilter {

    @Override
    public @Nullable String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {

        if (sourceUrl == null) {
            return urlToFilter;
        }

        final URL target;
        try {
            target = URLUtil.toURL(urlToFilter);
        } catch (MalformedURLException e) {
            // not for this filter to judge
            return urlToFilter;
        }

        if (isSameURL(sourceUrl, target)) {
            return null;
        }

        return urlToFilter;
    }

    /**
     * Only the scheme and the host are case-insensitive (RFC 3986, section 6.2.2.1), the path and
     * the query are not: a redirect from /a.html to /A.html points to another resource.
     */
    private static boolean isSameURL(URL source, URL target) {
        return source.getProtocol().equalsIgnoreCase(target.getProtocol())
                && source.getHost().equalsIgnoreCase(target.getHost())
                && source.getPort() == target.getPort()
                && source.getFile().equals(target.getFile())
                && Objects.equals(source.getRef(), target.getRef());
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode paramNode) {}
}
