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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    void missingQueueStreamKeysReportsAllWithDefaultConfig() {
        // with the default metadata.persist none of the keys survives the filter
        Set<String> missing =
                QueueRegulatorBolt.missingQueueStreamKeys(new HashMap<>(), RETRY_AFTER_KEY, false);
        assertEquals(Set.of("fetch.statusCode", RETRY_AFTER_KEY), missing);
    }

    @Test
    void missingQueueStreamKeysEmptyWhenAllPersisted() {
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

    @Test
    void robotsValidationIsSkippedWhenFrontierPacingIsDisabled() {
        assertDoesNotThrow(
                () -> QueueRegulatorBolt.validateRobotsPacingConfiguration(new HashMap<>()));
    }

    @Test
    void legacyFetcherForceDoesNotEnableFrontierPacing() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("fetcher.max.crawl.delay.force", true);
        assertDoesNotThrow(() -> QueueRegulatorBolt.validateRobotsPacingConfiguration(conf));
    }

    @Test
    void enabledRobotsPacingRequiresFetcherForce() {
        Map<String, Object> conf = validRobotsConf();
        conf.put("fetcher.max.crawl.delay.force", false);
        assertThrows(
                IllegalArgumentException.class,
                () -> QueueRegulatorBolt.validateRobotsPacingConfiguration(conf));
    }

    @Test
    void validPersistOnlyRobotsConfigurationPasses() {
        assertDoesNotThrow(
                () -> QueueRegulatorBolt.validateRobotsPacingConfiguration(validRobotsConf()));
    }

    @Test
    void robotsSignalMustSurvivePersistence() {
        Map<String, Object> conf = validRobotsConf();
        conf.remove("metadata.persist");
        assertThrows(
                IllegalArgumentException.class,
                () -> QueueRegulatorBolt.validateRobotsPacingConfiguration(conf));
    }

    @Test
    void robotsSignalMustNotTransferToOutlinks() {
        Map<String, Object> conf = validRobotsConf();
        conf.put("metadata.transfer", List.of("robots.*"));
        assertThrows(
                IllegalArgumentException.class,
                () -> QueueRegulatorBolt.validateRobotsPacingConfiguration(conf));
    }

    @Test
    void robotsPersistenceWildcardIsAccepted() {
        Map<String, Object> conf = validRobotsConf();
        conf.put("metadata.persist", List.of("robots.*"));
        assertDoesNotThrow(() -> QueueRegulatorBolt.validateRobotsPacingConfiguration(conf));
    }

    @Test
    void robotsPacingRequiresHostPartitioning() {
        for (String mode :
                List.of(
                        org.apache.stormcrawler.Constants.PARTITION_MODE_DOMAIN,
                        org.apache.stormcrawler.Constants.PARTITION_MODE_IP)) {
            Map<String, Object> conf = validRobotsConf();
            conf.put(org.apache.stormcrawler.Constants.PARTITION_MODEParamName, mode);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> QueueRegulatorBolt.validateRobotsPacingConfiguration(conf));
        }
    }

    @Test
    void robotsPacingRequiresSingleUrlHandouts() {
        Map<String, Object> defaultBatch = validRobotsConf();
        defaultBatch.remove(Constants.URLFRONTIER_MAX_URLS_PER_BUCKET_KEY);
        assertThrows(
                IllegalArgumentException.class,
                () -> QueueRegulatorBolt.validateRobotsPacingConfiguration(defaultBatch));

        Map<String, Object> multiUrlBatch = validRobotsConf();
        multiUrlBatch.put(Constants.URLFRONTIER_MAX_URLS_PER_BUCKET_KEY, 2);
        assertThrows(
                IllegalArgumentException.class,
                () -> QueueRegulatorBolt.validateRobotsPacingConfiguration(multiUrlBatch));
    }

    @Test
    void robotsPacingRequiresAnEnabledCrawlDelayLimit() {
        Map<String, Object> conf = validRobotsConf();
        conf.put("fetcher.max.crawl.delay", -1);
        assertThrows(
                IllegalArgumentException.class,
                () -> QueueRegulatorBolt.validateRobotsPacingConfiguration(conf));
    }

    @Test
    void robotsPacingRequiresAPositiveFrontierDelayCap() {
        Map<String, Object> conf = validRobotsConf();
        conf.put(Constants.URLFRONTIER_ROBOTS_DELAY_MAX_KEY, 0);
        assertThrows(
                IllegalArgumentException.class,
                () -> QueueRegulatorBolt.validateRobotsPacingConfiguration(conf));
    }

    @Test
    void robotsPacingRequiresASingleFrontierAddress() {
        // keyed setDelay is not propagated across nodes (crawler-commons/url-frontier#146)
        Map<String, Object> conf = validRobotsConf();
        conf.put(Constants.URLFRONTIER_ADDRESS_KEY, List.of("host1:7071", "host2:7071"));
        assertThrows(
                IllegalArgumentException.class,
                () -> QueueRegulatorBolt.validateRobotsPacingConfiguration(conf));

        Map<String, Object> single = validRobotsConf();
        single.put(Constants.URLFRONTIER_ADDRESS_KEY, List.of("host1:7071"));
        assertDoesNotThrow(() -> QueueRegulatorBolt.validateRobotsPacingConfiguration(single));
    }

    @Test
    void multipleFrontierAddressesAreAcceptedWhenRobotsPacingIsDisabled() {
        // the #1984 rate-limit blocks stay best-effort on the connected node: warn, do not fail
        Map<String, Object> conf = new HashMap<>();
        conf.put(Constants.URLFRONTIER_ADDRESS_KEY, List.of("host1:7071", "host2:7071"));
        assertDoesNotThrow(() -> QueueRegulatorBolt.validateRobotsPacingConfiguration(conf));
    }

    private static Map<String, Object> validRobotsConf() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Constants.URLFRONTIER_ROBOTS_CRAWL_DELAY_ENABLED_KEY, true);
        conf.put("fetcher.max.crawl.delay.force", true);
        conf.put(
                org.apache.stormcrawler.Constants.PARTITION_MODEParamName,
                org.apache.stormcrawler.Constants.PARTITION_MODE_HOST);
        conf.put(Constants.URLFRONTIER_MAX_URLS_PER_BUCKET_KEY, 1);
        conf.put(
                "metadata.persist",
                List.of(org.apache.stormcrawler.Constants.ROBOTS_CRAWL_DELAY_KEY));
        return conf;
    }
}
