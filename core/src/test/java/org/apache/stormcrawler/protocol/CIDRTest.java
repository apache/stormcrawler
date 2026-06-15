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

package org.apache.stormcrawler.protocol;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.net.InetAddresses;
import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class CIDRTest {

    private static InetAddress ip(String address) {
        return InetAddresses.forString(address);
    }

    @Test
    void singleIPv4AddressMatchesOnlyItself() {
        CIDR cidr = new CIDR("127.0.0.1");
        assertTrue(cidr.contains(ip("127.0.0.1")));
        assertFalse(cidr.contains(ip("127.0.0.2")));
        // a /32 must compare every byte, including the first one
        assertFalse(cidr.contains(ip("1.0.0.1")));
    }

    @Test
    void singleIPv6AddressMatchesOnlyItself() {
        CIDR cidr = new CIDR("::1");
        assertTrue(cidr.contains(ip("::1")));
        // a /128 must compare every byte, including the first one
        assertFalse(cidr.contains(ip("fd00::1")));
    }

    @Test
    void outOfRangeMaskThrows() {
        assertThrows(IllegalArgumentException.class, () -> new CIDR("10.0.0.0/-1"));
        assertThrows(IllegalArgumentException.class, () -> new CIDR("10.0.0.0/33"));
        assertThrows(IllegalArgumentException.class, () -> new CIDR("::1/129"));
    }

    @Test
    void ipv4CidrBlockMatchesAddressesInRange() {
        CIDR cidr = new CIDR("192.168.0.0/16");
        assertTrue(cidr.contains(ip("192.168.0.1")));
        assertTrue(cidr.contains(ip("192.168.255.255")));
        assertFalse(cidr.contains(ip("192.169.0.1")));
        assertFalse(cidr.contains(ip("10.0.0.1")));
    }

    @Test
    void ipv6CidrBlockMatchesAddressesInRange() {
        CIDR cidr = new CIDR("fd00::/8");
        assertTrue(cidr.contains(ip("fd00::1")));
        assertTrue(cidr.contains(ip("fdff::ffff")));
        assertFalse(cidr.contains(ip("fc00::1")));
    }

    @Test
    void ipv4AndIpv6AreNotComparable() {
        CIDR cidr = new CIDR("192.168.0.0/16");
        assertFalse(cidr.contains(ip("::1")));
    }

    @Test
    void invalidCidrThrows() {
        assertThrows(IllegalArgumentException.class, () -> new CIDR("not-an-ip"));
    }

    @Test
    void defaultMaskCoversFullAddress() {
        assertTrue(new CIDR("10.0.0.1").toString().endsWith("/32"));
    }
}
