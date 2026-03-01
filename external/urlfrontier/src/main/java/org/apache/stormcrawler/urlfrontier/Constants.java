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

package org.apache.stormcrawler.urlfrontier;

public final class Constants {
    private Constants() {}

    // General
    public static final String URLFRONTIER_ADDRESS_KEY = "urlfrontier.address";
    public static final String URLFRONTIER_HOST_KEY = "urlfrontier.host";
    public static final String URLFRONTIER_PORT_KEY = "urlfrontier.port";
    public static final String URLFRONTIER_DEFAULT_HOST = "localhost";
    public static final int URLFRONTIER_DEFAULT_PORT = 7071;

    // Spout
    public static final String URLFRONTIER_MAX_URLS_PER_BUCKET_KEY =
            "urlfrontier.max.urls.per.bucket";
    public static final String URLFRONTIER_MAX_BUCKETS_KEY = "urlfrontier.max.buckets";
    public static final String URLFRONTIER_DELAY_REQUESTABLE_KEY = "urlfrontier.delay.requestable";

    // StatusUpdater
    public static final String URLFRONTIER_CACHE_EXPIREAFTER_SEC_KEY =
            "urlfrontier.cache.expireafter.sec";
    public static final String URLFRONTIER_MAX_MESSAGES_IN_FLIGHT_KEY =
            "urlfrontier.max.messages.in.flight";
    public static final String URLFRONTIER_THROTTLING_TIME_MS_KEY =
            "urlfrontier.throttling.time.msec";
    public static final String URLFRONTIER_UPDATER_MAX_MESSAGES_KEY =
            "urlfrontier.updater.max.messages";
    public static final String URLFRONTIER_CRAWL_ID_KEY = "urlfrontier.crawlid";
}
