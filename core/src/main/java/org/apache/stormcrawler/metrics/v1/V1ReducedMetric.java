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

package org.apache.stormcrawler.metrics.v1;

import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.stormcrawler.metrics.ScopedReducedMetric;

/** V1 implementation of {@link ScopedReducedMetric} backed by {@link MultiReducedMetric}. */
public class V1ReducedMetric implements ScopedReducedMetric {

    private final MultiReducedMetric delegate;

    public V1ReducedMetric(MultiReducedMetric delegate) {
        this.delegate = delegate;
    }

    @Override
    public ReduceHandle scope(String scopeName) {
        return delegate.scope(scopeName)::update;
    }
}
