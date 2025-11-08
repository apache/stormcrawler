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

package org.apache.stormcrawler.protocol;

import crawlercommons.robots.BaseRobotRules;
import java.net.URL;
import java.util.List;

/**
 * Wrapper for BaseRobotRules which tracks the number of requests and length of the responses needed
 * to get the rules. If the array returned by getContentLengthFetched() is empty, then the rules
 * were obtained from the cache.
 */
public class RobotRules extends crawlercommons.robots.BaseRobotRules {

    private final BaseRobotRules base;
    private int[] bytesFetched = new int[] {};

    public RobotRules(BaseRobotRules base) {
        this.base = base;
    }

    @Override
    public boolean isAllowed(String url) {
        return base.isAllowed(url);
    }

    @Override
    public boolean isAllowed(URL url) {
        return base.isAllowed(url);
    }

    @Override
    public boolean isAllowAll() {
        return base.isAllowAll();
    }

    @Override
    public boolean isAllowNone() {
        return base.isAllowNone();
    }

    /** Returns the number of bytes fetched per request when not cached. */
    public int[] getContentLengthFetched() {
        return bytesFetched;
    }

    /** Returns the number of bytes fetched per request when not cached. */
    public void setContentLengthFetched(int[] bytesFetched) {
        this.bytesFetched = bytesFetched;
    }

    @Override
    public long getCrawlDelay() {
        return base.getCrawlDelay();
    }

    @Override
    public boolean isDeferVisits() {
        return base.isDeferVisits();
    }

    @Override
    public List<String> getSitemaps() {
        return base.getSitemaps();
    }

    @Override
    public int hashCode() {
        return base.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return base.equals(obj);
    }

    @Override
    public void setCrawlDelay(long crawlDelay) {
        base.setCrawlDelay(crawlDelay);
    }

    @Override
    public void setDeferVisits(boolean deferVisits) {
        base.setDeferVisits(deferVisits);
    }

    @Override
    public void addSitemap(String sitemap) {
        base.addSitemap(sitemap);
    }
}
