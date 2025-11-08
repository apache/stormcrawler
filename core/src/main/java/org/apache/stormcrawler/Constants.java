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

package org.apache.stormcrawler;

public class Constants {

    public static final String PARTITION_MODEParamName = "partition.url.mode";

    public static final String PARTITION_MODE_HOST = "byHost";
    public static final String PARTITION_MODE_DOMAIN = "byDomain";
    public static final String PARTITION_MODE_IP = "byIP";

    public static final String STATUS_ERROR_MESSAGE = "error.message";
    public static final String STATUS_ERROR_SOURCE = "error.source";
    public static final String STATUS_ERROR_CAUSE = "error.cause";

    public static final String StatusStreamName = "status";

    public static final String DELETION_STREAM_NAME = "deletion";

    public static final String AllowRedirParamName = "redirections.allowed";

    // when to retry a URL with a fetch error
    public static final String fetchErrorFetchIntervalParamName = "fetchInterval.fetch.error";

    // when to retry a URL with an error, i.e. something very wrong with it
    // set a very large value so that it does not get refetched soon
    public static final String errorFetchIntervalParamName = "fetchInterval.error";

    // when to retry a successful URL by default
    public static final String defaultFetchIntervalParamName = "fetchInterval.default";

    public static final String fetchErrorCountParamName = "fetch.error.count";

    /** Maximum array size, safe value on any JVM. */
    public static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private Constants() {}
}
