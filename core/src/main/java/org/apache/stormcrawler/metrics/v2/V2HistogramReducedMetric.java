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
import org.apache.stormcrawler.metrics.ScopedReducedMetric;

/**
 * V2 implementation of {@link ScopedReducedMetric} using Codahale {@link
 * com.codahale.metrics.Histogram} instances. Used as the V2 replacement for {@code
 * MultiReducedMetric(MeanReducer)}.
 */
public class V2HistogramReducedMetric implements ScopedReducedMetric {

    private final String baseName;
    private final TopologyContext context;
    private final ConcurrentMap<String, ReduceHandle> handles = new ConcurrentHashMap<>();

    public V2HistogramReducedMetric(String baseName, TopologyContext context) {
        this.baseName = baseName;
        this.context = context;
    }

    @Override
    public ReduceHandle scope(String scopeName) {
        return handles.computeIfAbsent(
                scopeName,
                name -> {
                    String metricName = baseName + "." + name;
                    com.codahale.metrics.Histogram histogram =
                            context.registerHistogram(metricName);
                    return value -> histogram.update(toLong(value));
                });
    }

    private static long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        throw new IllegalArgumentException(
                "V2HistogramReducedMetric: unsupported value type " + value.getClass());
    }
}
