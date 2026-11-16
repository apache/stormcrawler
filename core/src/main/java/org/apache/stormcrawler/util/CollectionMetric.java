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

import java.util.ArrayList;
import java.util.List;
import org.apache.storm.metric.api.IMetric;

public class CollectionMetric implements IMetric {

    private List<Long> measurements = new ArrayList<>();

    public void addMeasurement(long l) {
        synchronized (measurements) {
            measurements.add(l);
        }
    }

    /**
     * Returns the measurements taken since the last call as an {@link ArrayList}: a V1 metrics
     * consumer in another worker gets them through Kryo, which Storm sets up for {@link ArrayList}
     * but not for other lists.
     */
    @Override
    public Object getValueAndReset() {
        synchronized (measurements) {
            ArrayList<Long> copy = new ArrayList<>(measurements);
            measurements.clear();
            return copy;
        }
    }
}
