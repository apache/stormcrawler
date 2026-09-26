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

package org.apache.stormcrawler.urlfrontier;

import java.util.Arrays;
import java.util.function.LongSupplier;

/**
 * Cumulative distribution of the depth of the URLs handed out by the frontier over a sliding time
 * window. The window is a ring of {@value #SLOTS} slots of equal duration; a slot is reset the
 * first time it is written after a full turn of the ring, and a query sums the slots written within
 * the last window. Records and queries are constant time in the number of URLs.
 */
class DepthWindow {

    static final int SLOTS = 10;

    private final int maxDepth;
    private final long slotMillis;
    private final LongSupplier clock;

    /** Absolute slot number written in each slot of the ring, -1 when never written. */
    private final long[] slotEpoch = new long[SLOTS];

    /**
     * Per slot, indices 0 to maxDepth - 1 hold the number of URLs with a depth at most the index,
     * the last index the number of URLs with any depth.
     */
    private final long[][] counts;

    /**
     * @param maxDepth depths at or beyond this value only count in the total
     * @param windowMillis length of the window, must be at least {@value #SLOTS} ms
     * @param clock source of the current time in milliseconds
     */
    DepthWindow(int maxDepth, long windowMillis, LongSupplier clock) {
        if (windowMillis < SLOTS) {
            throw new IllegalArgumentException("window must be at least " + SLOTS + " ms");
        }
        this.maxDepth = maxDepth;
        this.slotMillis = windowMillis / SLOTS;
        this.clock = clock;
        this.counts = new long[SLOTS][maxDepth + 1];
        Arrays.fill(slotEpoch, -1);
    }

    /** Records a URL with the given non-negative depth at the current time. */
    synchronized void record(int depth) {
        long epoch = clock.getAsLong() / slotMillis;
        int slot = (int) (epoch % SLOTS);
        if (slotEpoch[slot] != epoch) {
            Arrays.fill(counts[slot], 0);
            slotEpoch[slot] = epoch;
        }
        long[] c = counts[slot];
        for (int i = depth; i < maxDepth; i++) {
            c[i]++;
        }
        c[maxDepth]++;
    }

    /**
     * Probability that a URL recorded within the window has a depth at most {@code depth}.
     *
     * @return a value between 0 and 1, or {@link Double#NaN} when nothing was recorded within the
     *     window
     */
    synchronized double probabilityAtMost(int depth) {
        long oldest = clock.getAsLong() / slotMillis - SLOTS + 1;
        long total = 0;
        long atMost = 0;
        int index = Math.min(depth, maxDepth);
        for (int slot = 0; slot < SLOTS; slot++) {
            if (slotEpoch[slot] >= oldest) {
                total += counts[slot][maxDepth];
                if (index >= 0) {
                    atMost += counts[slot][index];
                }
            }
        }
        if (total == 0) {
            return Double.NaN;
        }
        return (double) atMost / total;
    }
}
