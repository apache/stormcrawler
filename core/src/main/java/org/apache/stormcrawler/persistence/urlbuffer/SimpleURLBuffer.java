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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple implementation of a URLBuffer which rotates on the queues without applying any priority.
 *
 * @since 1.15
 */
public class SimpleURLBuffer extends AbstractURLBuffer {

    static final Logger LOG = LoggerFactory.getLogger(SimpleURLBuffer.class);

    /**
     * Retrieves the next available URL, guarantees that the URLs are always perfectly shuffled.
     *
     * @return null if no entries are available
     */
    public synchronized Values next() {

        if (queues.isEmpty()) {
            return null;
        }

        Iterator<Entry<String, Queue<URLMetadata>>> i = queues.entrySet().iterator();

        if (!i.hasNext()) {
            LOG.debug("Empty iterator");
            return null;
        }

        Map.Entry<String, Queue<URLMetadata>> nextEntry = i.next();

        Queue<URLMetadata> queue = nextEntry.getValue();
        String queueName = nextEntry.getKey();

        // remove the entry
        i.remove();

        LOG.debug("Next queue {}", queueName);

        // remove the first element
        URLMetadata item = queue.poll();

        if (item == null) {
            LOG.debug("The queue at {} is empty.", queueName);
            return null;
        }

        LOG.debug("Item {}", item.url);

        // any left? add to the end of the iterator
        if (!queue.isEmpty()) {
            LOG.debug("adding to the back of the queue {}", queueName);
            queues.put(queueName, queue);
        } else {
            // notify that the queue is empty
            if (listener != null) {
                listener.emptyQueue(queueName);
            }
        }

        // remove it from the list of URLs in the queue
        inBuffer.remove(item.url);
        return new Values(item.url, item.metadata);
    }
}
