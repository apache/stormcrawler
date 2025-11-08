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

import org.apache.storm.metric.api.IReducer;

/** Used to return an average value per second. */
public class PerSecondReducer implements IReducer<TimeReducerState> {

    @Override
    public TimeReducerState init() {
        return new TimeReducerState();
    }

    @Override
    public TimeReducerState reduce(TimeReducerState accumulator, Object input) {
        if (input instanceof Double) {
            accumulator.sum += (Double) input;
        } else if (input instanceof Long) {
            accumulator.sum += ((Long) input).doubleValue();
        } else if (input instanceof Integer) {
            accumulator.sum += ((Integer) input).doubleValue();
        } else {
            throw new RuntimeException(
                    "MeanReducer::reduce called with unsupported input type `"
                            + input.getClass()
                            + "`. Supported types are Double, Long, Integer.");
        }
        return accumulator;
    }

    @Override
    public Object extractResult(TimeReducerState accumulator) {
        // time spent
        double msec = System.currentTimeMillis() - accumulator.started;
        if (msec == 0) {
            return 0;
        }
        double permsec = accumulator.sum / msec;
        return permsec * 1000d;
    }
}

class TimeReducerState {
    public long started = System.currentTimeMillis();
    public double sum = 0.0;
}
