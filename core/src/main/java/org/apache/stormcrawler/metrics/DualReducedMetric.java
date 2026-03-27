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

package org.apache.stormcrawler.metrics;

/**
 * Forwards reduced metric operations to both a V1 and a V2 {@link ScopedReducedMetric}
 * simultaneously, allowing both metric pipelines to receive data during migration.
 */
public class DualReducedMetric implements ScopedReducedMetric {

    private final ScopedReducedMetric v1;
    private final ScopedReducedMetric v2;

    public DualReducedMetric(ScopedReducedMetric v1, ScopedReducedMetric v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    @Override
    public ReduceHandle scope(String scopeName) {
        ReduceHandle v1Handle = v1.scope(scopeName);
        ReduceHandle v2Handle = v2.scope(scopeName);
        return value -> {
            v1Handle.update(value);
            v2Handle.update(value);
        };
    }
}
