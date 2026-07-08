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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Unit coverage for {@link QueueRegulatorBolt#missingQueueStreamKeys(Map, String, boolean)} - the
 * startup check that the metadata keys the bolt relies on survive the {@code metadata.persist}
 * filter applied by the status updater. The block decision itself is covered by {@link
 * HostBackoffTest}.
 */
class QueueRegulatorBoltDecisionTest {

    private static final String RETRY_AFTER_KEY = "protocol.retry-after";

    @Test
    void missingQueueStreamKeysReportsBothWithDefaultConfig() {
        // with the default metadata.persist neither key survives the filter
        Set<String> missing =
                QueueRegulatorBolt.missingQueueStreamKeys(new HashMap<>(), RETRY_AFTER_KEY, false);
        assertEquals(Set.of("fetch.statusCode", RETRY_AFTER_KEY), missing);
    }

    @Test
    void missingQueueStreamKeysEmptyWhenBothPersisted() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("metadata.persist", List.of("fetch.statusCode", RETRY_AFTER_KEY));
        assertTrue(
                QueueRegulatorBolt.missingQueueStreamKeys(conf, RETRY_AFTER_KEY, false).isEmpty());
    }

    @Test
    void missingQueueStreamKeysHonoursWildcards() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("metadata.persist", List.of("fetch.*", "protocol.*"));
        assertTrue(
                QueueRegulatorBolt.missingQueueStreamKeys(conf, RETRY_AFTER_KEY, false).isEmpty());
    }

    @Test
    void exceptionKeyOnlyRequiredWhenBackoffOnExceptionsIsEnabled() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("metadata.persist", List.of("fetch.statusCode", RETRY_AFTER_KEY));
        // the same configuration is complete without the exceptions gate and
        // incomplete with it
        assertTrue(
                QueueRegulatorBolt.missingQueueStreamKeys(conf, RETRY_AFTER_KEY, false).isEmpty());
        assertEquals(
                Set.of("fetch.exception"),
                QueueRegulatorBolt.missingQueueStreamKeys(conf, RETRY_AFTER_KEY, true));
    }
}
