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

package org.apache.stormcrawler.metrics.v2;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.metrics.ScopedCounter;

/**
 * V2 implementation of {@link ScopedCounter} using Codahale {@link com.codahale.metrics.Counter}
 * instances registered via the Storm V2 metrics API.
 */
public class V2CounterMetric implements ScopedCounter {

    private final String baseName;
    private final TopologyContext context;
    private final ConcurrentMap<String, CountHandle> handles = new ConcurrentHashMap<>();

    public V2CounterMetric(String baseName, TopologyContext context) {
        this.baseName = baseName;
        this.context = context;
    }

    @Override
    public CountHandle scope(String scopeName) {
        return handles.computeIfAbsent(
                scopeName,
                name -> {
                    String metricName = baseName + "." + name;
                    com.codahale.metrics.Counter counter = context.registerCounter(metricName);
                    return counter::inc;
                });
    }
}
