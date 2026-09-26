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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Cumulative depth distribution over a sliding time window. */
class DepthWindowTest {

    private static final int MAX_DEPTH = 10;
    private static final long WINDOW_MS = 100_000;

    private AtomicLong clock;
    private DepthWindow window;

    @BeforeEach
    void setUp() {
        clock = new AtomicLong(1_000_000);
        window = new DepthWindow(MAX_DEPTH, WINDOW_MS, clock::get);
    }

    @Test
    void isNaNWithoutData() {
        assertTrue(Double.isNaN(window.probabilityAtMost(0)));
    }

    @Test
    void cumulativeShareOfRecentUrls() {
        window.record(0);
        window.record(1);
        window.record(1);
        window.record(3);

        assertEquals(0.25, window.probabilityAtMost(0), 1e-9);
        assertEquals(0.75, window.probabilityAtMost(2), 1e-9);
        assertEquals(1.0, window.probabilityAtMost(3), 1e-9);
        assertEquals(1.0, window.probabilityAtMost(MAX_DEPTH + 5), 1e-9);
    }

    @Test
    void depthsBeyondTheCapCountOnlyInTheTotal() {
        window.record(2);
        window.record(250);

        assertEquals(0.5, window.probabilityAtMost(MAX_DEPTH - 1), 1e-9);
        assertEquals(1.0, window.probabilityAtMost(MAX_DEPTH), 1e-9);
    }

    @Test
    void urlsOlderThanTheWindowAreForgotten() {
        window.record(0);
        window.record(0);
        clock.addAndGet(WINDOW_MS / 2);
        window.record(5);
        assertEquals(2.0 / 3, window.probabilityAtMost(0), 1e-9);

        // the first two records fall out, the third is still inside
        clock.addAndGet(WINDOW_MS / 2 + WINDOW_MS / 10);
        assertEquals(0.0, window.probabilityAtMost(0), 1e-9);
        assertEquals(1.0, window.probabilityAtMost(5), 1e-9);

        // nothing left
        clock.addAndGet(WINDOW_MS);
        assertTrue(Double.isNaN(window.probabilityAtMost(5)));
    }

    @Test
    void slotReusedAfterFullTurnStartsFromZero() {
        window.record(0);
        clock.addAndGet(WINDOW_MS); // same slot index, one turn later
        window.record(7);

        assertEquals(0.0, window.probabilityAtMost(0), 1e-9);
        assertEquals(1.0, window.probabilityAtMost(7), 1e-9);
    }
}
