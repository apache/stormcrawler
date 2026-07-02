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

package org.apache.stormcrawler.persistence.urlbuffer;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.EmptyQueueListener;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.InitialisationUtil;
import org.jetbrains.annotations.NotNull;

/**
 * Buffers URLs to be processed into separate queues; used by spouts. Guarantees that no URL can be
 * put in the buffer more than once.
 *
 * <p>Configured by setting
 *
 * <p>urlbuffer.class: "org.apache.stormcrawler.persistence.SimpleURLBuffer"
 *
 * <p>in the configuration
 *
 * @since 1.15
 */
public interface URLBuffer {

    /** Implementation to use for URLBuffer. Must implement the interface URLBuffer. */
    String bufferClassParamName = "urlbuffer.class";

    /** Returns a URLBuffer instance based on the configuration. */
    static @NotNull URLBuffer createInstance(@NotNull Map<String, Object> stormConf) {

        String className = ConfUtils.getString(stormConf, bufferClassParamName);
        if (StringUtils.isBlank(className)) {
            throw new RuntimeException("Missing value for config  " + bufferClassParamName);
        }

        URLBuffer buffer;
        try {
            buffer = InitialisationUtil.initializeFromQualifiedName(className, URLBuffer.class);
            buffer.configure(stormConf);
        } catch (Exception e) {
            throw new RuntimeException("Can't instantiate " + className, e);
        }

        return buffer;
    }

    /** Replace with {@link URLBuffer#createInstance(Map)}. */
    @Deprecated
    static URLBuffer getInstance(Map<String, Object> stormConf) {
        return URLBuffer.createInstance(stormConf);
    }

    /**
     * Stores the URL and its Metadata under a given key.
     *
     * <p>Implementations of this method should be synchronised
     *
     * @return false if the URL was already in the buffer, true if it wasn't and was added
     */
    boolean add(String url, Metadata m, String key);

    /**
     * Stores the URL and its Metadata using the hostname as key.
     *
     * <p>Implementations of this method should be synchronised
     *
     * @return false if the URL was already in the buffer, true if it wasn't and was added
     */
    default boolean add(String url, Metadata m) {
        return add(url, m, null);
    }

    /** Total number of URLs in the buffer. */
    int size();

    /** Total number of queues in the buffer. */
    int numQueues();

    /**
     * Retrieves the next available URL, guarantees that the URLs are always perfectly shuffled.
     *
     * <p>Implementations of this method should be synchronised
     */
    Values next();

    /** Implementations of this method should be synchronised. */
    boolean hasNext();

    void setEmptyQueueListener(EmptyQueueListener l);

    /**
     * Notify the buffer that a URL has been successfully processed used e.g to compute an ideal
     * delay for a host queue.
     */
    default void acked(String url) {
        // do nothing with the information about URLs being acked
    }

    default void configure(Map<String, Object> stormConf) {}
}
