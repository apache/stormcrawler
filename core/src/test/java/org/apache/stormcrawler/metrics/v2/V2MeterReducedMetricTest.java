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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Meter;
import org.apache.storm.task.TopologyContext;
import org.apache.stormcrawler.metrics.ScopedReducedMetric;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class V2MeterReducedMetricTest {

    private TopologyContext context;
    private Meter meter;
    private V2MeterReducedMetric metric;

    @BeforeEach
    void setUp() {
        context = mock(TopologyContext.class);
        meter = new Meter();
        when(context.registerMeter(anyString())).thenReturn(meter);
        metric = new V2MeterReducedMetric("test_meter", context);
    }

    @Test
    void scopeRegistersMeterWithComposedName() {
        metric.scope("myScope");
        verify(context).registerMeter("test_meter.myScope");
    }

    @Test
    void scopeReturnsSameHandleForSameName() {
        ScopedReducedMetric.ReduceHandle first = metric.scope("s1");
        ScopedReducedMetric.ReduceHandle second = metric.scope("s1");
        assertSame(first, second);
    }

    @Test
    void updateMarksMeterWithLongValue() {
        metric.scope("s1").update(5L);
        assertEquals(5, meter.getCount());
    }

    @Test
    void updateAcceptsIntegerValues() {
        metric.scope("s1").update(3);
        assertEquals(3, meter.getCount());
    }

    @Test
    void updateThrowsOnNonNumericValue() {
        ScopedReducedMetric.ReduceHandle handle = metric.scope("s1");
        assertThrows(IllegalArgumentException.class, () -> handle.update("not a number"));
    }
}
