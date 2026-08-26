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

package org.apache.stormcrawler.persistence;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class StatusTest {

    @Test
    void testPermanentRedirects() {
        assertTrue(Status.isPermanentRedirect(301));
        assertTrue(Status.isPermanentRedirect(308));
    }

    @Test
    void testNonPermanentRedirects() {
        assertFalse(Status.isPermanentRedirect(300));
        assertFalse(Status.isPermanentRedirect(302));
        assertFalse(Status.isPermanentRedirect(303));
        assertFalse(Status.isPermanentRedirect(307));
        assertFalse(Status.isPermanentRedirect(200));
        assertFalse(Status.isPermanentRedirect(404));
    }
}
