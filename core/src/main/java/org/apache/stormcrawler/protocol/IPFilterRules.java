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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.commons.lang3.StringUtils;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optionally limit or block connections to IP address ranges (localhost/loopback or site-local
 * addresses, subnet ranges given in CIDR notation, or single IP addresses).
 *
 * <p>IP filter rules are built from two configuration properties:
 *
 * <ul>
 *   <li><code>http.filter.ipaddress.include</code> defines all allowed IP ranges. If not defined or
 *       empty all IP addresses (and not explicitly excluded) are allowed.
 *   <li><code>http.filter.ipaddress.exclude</code> defines excluded IP address ranges.
 * </ul>
 *
 * <p>IP ranges can be defined as
 *
 * <ul>
 *   <li>IP address, e.g. <code>127.0.0.1</code> or <code>::1</code> (IPv6)
 *   <li>CIDR notation, e.g. <code>192.168.0.0/16</code> or <code>fd00::/8</code>
 *   <li><code>localhost</code> or <code>loopback</code> applies to all IP addresses for which
 *       {@link InetAddress#isLoopbackAddress()} is true
 *   <li><code>sitelocal</code> applies to all IP addresses for which {@link
 *       InetAddress#isSiteLocalAddress()} is true
 * </ul>
 *
 * <p>Multiple IP ranges can be given either as a comma-separated string, e.g. <code>
 * loopback,sitelocal,fd00::/8</code>, or as a list in the configuration.
 */
public class IPFilterRules {

    protected static final Logger LOG = LoggerFactory.getLogger(IPFilterRules.class);

    public static final String INCLUDE_RULES_KEY = "http.filter.ipaddress.include";
    public static final String EXCLUDE_RULES_KEY = "http.filter.ipaddress.exclude";

    private final List<Predicate<InetAddress>> includeRules;
    private final List<Predicate<InetAddress>> excludeRules;

    public IPFilterRules(Map<String, Object> conf) {
        includeRules = parseIPRules(conf, INCLUDE_RULES_KEY);
        excludeRules = parseIPRules(conf, EXCLUDE_RULES_KEY);
    }

    public boolean isEmpty() {
        return includeRules.isEmpty() && excludeRules.isEmpty();
    }

    public boolean accept(InetAddress address) {
        if (address == null) {
            return false;
        }
        boolean accept = true;
        if (!includeRules.isEmpty()) {
            accept = false;
            for (Predicate<InetAddress> rule : includeRules) {
                if (rule.test(address)) {
                    accept = true;
                    break;
                }
            }
        }
        if (accept && !excludeRules.isEmpty()) {
            for (Predicate<InetAddress> rule : excludeRules) {
                if (rule.test(address)) {
                    accept = false;
                    break;
                }
            }
        }
        return accept;
    }

    private static List<Predicate<InetAddress>> parseIPRules(
            Map<String, Object> conf, String ipRuleProperty) {
        List<Predicate<InetAddress>> rules = new ArrayList<>();
        for (String entry : ConfUtils.loadListFromConf(ipRuleProperty, conf)) {
            // a single config value may itself hold a comma-separated list of rules
            for (String ipRule : entry.split(",")) {
                ipRule = ipRule.trim();
                if (StringUtils.isBlank(ipRule)) {
                    continue;
                }
                switch (ipRule.toLowerCase(Locale.ROOT)) {
                    case "localhost":
                    case "loopback":
                        rules.add(InetAddress::isLoopbackAddress);
                        break;
                    case "sitelocal":
                        rules.add(InetAddress::isSiteLocalAddress);
                        break;
                    default:
                        try {
                            CIDR cidr = new CIDR(ipRule);
                            rules.add(cidr::contains);
                        } catch (IllegalArgumentException e) {
                            LOG.error(
                                    "Failed to parse {} as CIDR, ignoring it while configuring IP rules ({})",
                                    ipRule,
                                    ipRuleProperty);
                        }
                }
            }
        }
        if (!rules.isEmpty()) {
            LOG.info("Found {} IP filter rule(s) for {}", rules.size(), ipRuleProperty);
        }
        return rules;
    }
}
