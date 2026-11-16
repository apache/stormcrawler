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

package org.apache.stormcrawler.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import java.util.LinkedList;
import java.util.List;
import org.apache.storm.serialization.SerializationFactory;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

class CollectionMetricTest {

    /** Kryo as Storm sets it up for a topology that registers no classes of its own. */
    private static Kryo stormKryo() {
        return SerializationFactory.getKryo(Utils.readDefaultConfig());
    }

    @Test
    void returnsTheMeasurementsSinceTheLastCall() {
        final CollectionMetric metric = new CollectionMetric();
        metric.addMeasurement(3);
        metric.addMeasurement(5);
        assertEquals(List.of(3L, 5L), metric.getValueAndReset());
        assertEquals(List.of(), metric.getValueAndReset());
    }

    @Test
    void measurementsCanBeSentToAMetricsConsumerInAnotherWorker() {
        final CollectionMetric metric = new CollectionMetric();
        metric.addMeasurement(3);
        final Kryo kryo = stormKryo();
        assertDoesNotThrow(
                () -> kryo.writeClassAndObject(new Output(1024), metric.getValueAndReset()));
    }

    @Test
    void linkedListCannotBeSent() {
        // why getValueAndReset() copies the measurements into an ArrayList
        final Kryo kryo = stormKryo();
        assertThrows(
                IllegalArgumentException.class,
                () -> kryo.writeClassAndObject(new Output(1024), new LinkedList<Long>()));
    }
}
