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
 * Forwards counter operations to both a V1 and a V2 {@link ScopedCounter} simultaneously, allowing
 * both metric pipelines to receive data during migration.
 */
public class DualCounterMetric implements ScopedCounter {

    private final ScopedCounter v1;
    private final ScopedCounter v2;

    public DualCounterMetric(ScopedCounter v1, ScopedCounter v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    @Override
    public CountHandle scope(String scopeName) {
        CountHandle v1Handle = v1.scope(scopeName);
        CountHandle v2Handle = v2.scope(scopeName);
        return incrementBy -> {
            v1Handle.incrBy(incrementBy);
            v2Handle.incrBy(incrementBy);
        };
    }
}
