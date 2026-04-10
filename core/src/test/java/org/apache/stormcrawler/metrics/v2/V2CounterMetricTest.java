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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Counter;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V2CounterMetricTest {

    private TopologyContext context;
    private Counter counter;
    private V2CounterMetric metric;

    @BeforeEach
    void setUp() {
        context = mock(TopologyContext.class);
        counter = new Counter();
        when(context.registerCounter(anyString())).thenReturn(counter);
        metric = new V2CounterMetric("test_counter", context);
    }

    @Test
    void scopeRegistersCounterWithComposedName() {
        metric.scope("myScope");
        verify(context).registerCounter("test_counter.myScope");
    }

    @Test
    void scopeReturnsSameHandleForSameName() {
        ScopedCounter.CountHandle first = metric.scope("s1");
        ScopedCounter.CountHandle second = metric.scope("s1");
        assertSame(first, second);
    }

    @Test
    void incrByDelegatesToCodahaleCounter() {
        ScopedCounter.CountHandle handle = metric.scope("s1");
        handle.incrBy(7);
        assertEquals(7, counter.getCount());

        handle.incr();
        assertEquals(8, counter.getCount());
    }

    @Test
    void differentScopesGetDifferentCounters() {
        Counter counter2 = new Counter();
        when(context.registerCounter("test_counter.a")).thenReturn(counter);
        when(context.registerCounter("test_counter.b")).thenReturn(counter2);

        metric.scope("a").incrBy(3);
        metric.scope("b").incrBy(5);

        assertEquals(3, counter.getCount());
        assertEquals(5, counter2.getCount());
    }
}
