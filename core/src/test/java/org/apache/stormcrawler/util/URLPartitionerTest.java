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

import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Host aliases of one server (percent-escaping, case, trailing dot) must get the same partition key
 * in every mode, otherwise they end up in separate politeness queues.
 */
class URLPartitionerTest {

    private static final Metadata EMPTY = new Metadata();

    @Test
    void byHostCollapsesHostAliases() {
        Assertions.assertEquals(
                "example.org",
                URLPartitioner.getPartition(
                        "http://example.org/a", EMPTY, Constants.PARTITION_MODE_HOST));
        Assertions.assertEquals(
                "example.org",
                URLPartitioner.getPartition(
                        "http://exampl%65.org/a", EMPTY, Constants.PARTITION_MODE_HOST));
        Assertions.assertEquals(
                "example.org",
                URLPartitioner.getPartition(
                        "http://example.org./a", EMPTY, Constants.PARTITION_MODE_HOST));
        Assertions.assertEquals(
                "example.org",
                URLPartitioner.getPartition(
                        "http://EXAMPLE.org/a", EMPTY, Constants.PARTITION_MODE_HOST));
    }

    @Test
    void byDomainCollapsesHostAliases() {
        Assertions.assertEquals(
                "example.org",
                URLPartitioner.getPartition(
                        "http://example.org/a", EMPTY, Constants.PARTITION_MODE_DOMAIN));
        Assertions.assertEquals(
                "example.org",
                URLPartitioner.getPartition(
                        "http://exampl%65.org/a", EMPTY, Constants.PARTITION_MODE_DOMAIN));
        Assertions.assertEquals(
                "example.org",
                URLPartitioner.getPartition(
                        "http://www.example.org./a", EMPTY, Constants.PARTITION_MODE_DOMAIN));
    }

    @Test
    void byIPCollapsesHostAliases() {
        String canonical =
                URLPartitioner.getPartition(
                        "http://example.org/a", EMPTY, Constants.PARTITION_MODE_IP);
        Assertions.assertNotNull(canonical);
        Assertions.assertEquals(
                canonical,
                URLPartitioner.getPartition(
                        "http://exampl%65.org/a", EMPTY, Constants.PARTITION_MODE_IP));
        Assertions.assertEquals(
                canonical,
                URLPartitioner.getPartition(
                        "http://example.org./a", EMPTY, Constants.PARTITION_MODE_IP));
    }

    /** An explicitly supplied IP is used as the key unchanged, whatever the URL spells. */
    @Test
    void byIPKeepsProvidedIP() {
        Metadata metadata = new Metadata();
        metadata.setValue("ip", "192.0.2.1");
        Assertions.assertEquals(
                "192.0.2.1",
                URLPartitioner.getPartition(
                        "http://exampl%65.org/a", metadata, Constants.PARTITION_MODE_IP));
        Assertions.assertEquals(
                "192.0.2.1",
                URLPartitioner.getPartition(
                        "http://example.org./a", metadata, Constants.PARTITION_MODE_IP));
    }
}
