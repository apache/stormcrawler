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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.EmptyQueueListener;
import org.apache.stormcrawler.util.URLPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for URLBuffer interface, meant to simplify the code of the implementations and
 * provide some default methods
 *
 * @since 1.15
 */
public abstract class AbstractURLBuffer implements URLBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractURLBuffer.class);

    protected Set<String> in_buffer = new HashSet<>();
    protected EmptyQueueListener listener = null;

    protected final URLPartitioner partitioner = new URLPartitioner();

    protected final Map<String, Queue<URLMetadata>> queues =
            Collections.synchronizedMap(new LinkedHashMap<>());

    public void configure(Map<String, Object> stormConf) {
        partitioner.configure(stormConf);
    }

    /** Total number of queues in the buffer * */
    public synchronized int numQueues() {
        return queues.size();
    }

    /**
     * Stores the URL and its Metadata under a given key.
     *
     * @return false if the URL was already in the buffer, true if it wasn't and was added
     */
    public synchronized boolean add(String URL, Metadata m, String key) {

        LOG.debug("Adding {}", URL);

        if (in_buffer.contains(URL)) {
            LOG.debug("already in buffer {}", URL);
            return false;
        }

        // determine which queue to use
        // configure with other than hostname
        if (key == null) {
            key = partitioner.getPartition(URL, m);
            if (key == null) {
                key = "_DEFAULT_";
            }
        }

        // create the queue if it does not exist
        // and add the url
        queues.computeIfAbsent(key, k -> new LinkedList<>()).add(new URLMetadata(URL, m));
        return in_buffer.add(URL);
    }

    /**
     * Stores the URL and its Metadata using the hostname as key.
     *
     * @return false if the URL was already in the buffer, true if it wasn't and was added
     */
    public synchronized boolean add(String URL, Metadata m) {
        return add(URL, m, null);
    }

    /** Total number of URLs in the buffer * */
    public synchronized int size() {
        return in_buffer.size();
    }

    public void setEmptyQueueListener(EmptyQueueListener l) {
        listener = l;
    }

    @Override
    public synchronized boolean hasNext() {
        return !queues.isEmpty();
    }

    protected class URLMetadata {
        String url;
        Metadata metadata;

        URLMetadata(String u, Metadata m) {
            url = u;
            metadata = m;
        }
    }
}
