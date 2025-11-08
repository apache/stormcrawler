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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Determines the priority of the buffers based on the number of URLs acked in a configurable period
 * of time.
 *
 * @since 1.16
 */
public class PriorityURLBuffer extends SimpleURLBuffer {

    static final Logger LOG = LoggerFactory.getLogger(PriorityURLBuffer.class);

    private Map<String, AtomicInteger> ackCount;

    private Instant lastSorting;

    private long minDelayReRankSec = 10L;

    public PriorityURLBuffer() {
        lastSorting = Instant.now();
        ackCount = new ConcurrentHashMap<>();
    }

    public synchronized Values next() {
        // check whether we need to re-rank the buffers
        if (lastSorting.plusSeconds(minDelayReRankSec).isBefore(Instant.now())) {
            rerank();
        }
        return super.next();
    }

    // sorts the buffers according to the number of acks they got since the
    // previous time
    private void rerank() {
        if (queues.isEmpty()) {
            return;
        }

        List<QueueCount> sorted = new ArrayList<>();

        for (Entry<String, Queue<URLMetadata>> entry : queues.entrySet()) {
            String name = entry.getKey();
            int ackNum = ackCount.getOrDefault(name, new AtomicInteger(0)).get();
            QueueCount qc = new QueueCount(name, ackNum);
            sorted.add(qc);
        }

        Collections.sort(sorted);

        // now create a new Map of queues based on the sorted set
        // lowest score go first

        for (QueueCount q : sorted) {
            Queue<URLMetadata> queue = queues.remove(q.name);
            queues.put(q.name, queue);
        }

        ackCount.clear();
        lastSorting = Instant.now();
    }

    public void acked(String url) {
        // get the queue for this URL
        String key = partitioner.getPartition(url, Metadata.empty);
        if (key == null) {
            key = "_DEFAULT_";
        }
        // increment the counter for it
        ackCount.computeIfAbsent(key, k -> new AtomicInteger(0)).incrementAndGet();
    }

    class QueueCount implements Comparable<QueueCount> {
        String name;
        int count;

        QueueCount(String n, int c) {
            name = n;
            count = c;
        }

        @Override
        public int compareTo(QueueCount otherQueue) {
            int countDiff = otherQueue.count - count;
            if (countDiff == 0) {
                return name.compareTo(otherQueue.name);
            }
            return countDiff;
        }
    }
}
