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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class DualCounterMetricTest {

    @Test
    void scopeDelegatesToBothCounters() {
        AtomicLong v1Total = new AtomicLong();
        AtomicLong v2Total = new AtomicLong();

        ScopedCounter v1 = scopeName -> v1Total::addAndGet;
        ScopedCounter v2 = scopeName -> v2Total::addAndGet;

        DualCounterMetric dual = new DualCounterMetric(v1, v2);
        ScopedCounter.CountHandle handle = dual.scope("test");

        handle.incrBy(5);
        assertEquals(5, v1Total.get());
        assertEquals(5, v2Total.get());

        handle.incrBy(3);
        assertEquals(8, v1Total.get());
        assertEquals(8, v2Total.get());
    }
}
