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

import org.apache.stormcrawler.Metadata;

/**
 * Normalises the robots instructions provided by the HTML meta tags or the HTTP X-Robots-Tag
 * headers.
 */
public class RobotsTags {

    public static final String ROBOTS_NO_INDEX = "robots.noIndex";

    public static final String ROBOTS_NO_FOLLOW = "robots.noFollow";

    /**
     * Whether to interpret the noFollow directive strictly (remove links) or not (remove anchor and
     * do not track original URL). True by default.
     */
    public static final String ROBOTS_NO_FOLLOW_STRICT = "robots.noFollow.strict";

    public static final String ROBOTS_NO_CACHE = "robots.noCache";

    private boolean noIndex = false;

    private boolean noFollow = false;

    private boolean noCache = false;

    /** Get the values from the fetch metadata. */
    public RobotsTags(Metadata metadata, String protocolMDprefix) {
        // HTTP headers
        // X-Robots-Tag: noindex
        String[] values = metadata.getValues("X-Robots-Tag", protocolMDprefix);
        if (values == null) {
            return;
        }
        if (values.length == 1) {
            // just in case they put all the values on a single line
            values = values[0].split(" *, *");
        }
        parseValues(values);
    }

    public RobotsTags() {}

    /** Extracts meta tags based on the value of the content attribute. */
    public void extractMetaTags(String content) {
        if (content == null) {
            return;
        }
        String[] vals = content.split(" *, *");
        parseValues(vals);
    }

    private void parseValues(String[] values) {
        for (String v : values) {
            v = v.trim();
            if ("noindex".equalsIgnoreCase(v)) {
                noIndex = true;
            } else if ("nofollow".equalsIgnoreCase(v)) {
                noFollow = true;
            } else if ("noarchive".equalsIgnoreCase(v)) {
                noCache = true;
            } else if ("none".equalsIgnoreCase(v)) {
                noIndex = true;
                noFollow = true;
                noCache = true;
            }
        }
    }

    /** Adds a normalised representation of the directives in the metadata. */
    public void normaliseToMetadata(Metadata metadata) {
        metadata.setValue(ROBOTS_NO_INDEX, Boolean.toString(noIndex));
        metadata.setValue(ROBOTS_NO_CACHE, Boolean.toString(noCache));
        metadata.setValue(ROBOTS_NO_FOLLOW, Boolean.toString(noFollow));
    }

    public boolean isNoIndex() {
        return noIndex;
    }

    public boolean isNoFollow() {
        return noFollow;
    }

    public boolean isNoCache() {
        return noCache;
    }
}
