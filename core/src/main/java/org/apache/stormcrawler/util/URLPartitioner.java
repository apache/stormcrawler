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

import crawlercommons.domains.PaidLevelDomain;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a partition key for a given URL based on the hostname, domain or IP address. This can
 * be called by the URLPartitionerBolt or any other component.
 */
public class URLPartitioner {

    private static final Logger LOG = LoggerFactory.getLogger(URLPartitioner.class);

    private String mode = Constants.PARTITION_MODE_HOST;

    /**
     * Returns the host, domain, IP of a URL so that it can be partitioned for politeness, depending
     * on the value of the parameter <i>partitionMode</i>.
     */
    public static String getPartition(
            final String url, final Metadata metadata, final String partitionMode) {
        String partitionKey = null;
        String host = "";

        // IP in metadata?
        if (partitionMode.equalsIgnoreCase(Constants.PARTITION_MODE_IP)) {
            String ipProvided = metadata.getFirstValue("ip");
            if (StringUtils.isNotBlank(ipProvided)) {
                partitionKey = ipProvided;
            }
        }

        if (partitionKey == null) {
            URL u;
            try {
                u = new URL(url);
                host = u.getHost();
            } catch (MalformedURLException e1) {
                LOG.warn("Invalid URL: {}", url);
                return null;
            }
        }

        // partition by hostname
        if (partitionMode.equalsIgnoreCase(Constants.PARTITION_MODE_HOST)) {
            partitionKey = host;
        } else if (partitionMode.equalsIgnoreCase(Constants.PARTITION_MODE_DOMAIN)) {
            // partition by domain : needs fixing
            partitionKey = PaidLevelDomain.getPLD(host);
        }

        // partition by IP
        if (partitionMode.equalsIgnoreCase(Constants.PARTITION_MODE_IP) && partitionKey == null) {
            try {
                long start = System.currentTimeMillis();
                final InetAddress addr = InetAddress.getByName(host);
                partitionKey = addr.getHostAddress();
                long end = System.currentTimeMillis();
                LOG.debug("Resolved IP {} in {} msec for : {}", partitionKey, end - start, url);
            } catch (final Exception e) {
                LOG.warn("Unable to resolve IP for: {}", host);
                return null;
            }
        }

        LOG.debug("Partition Key for: {} > {}", url, partitionKey);

        return partitionKey;
    }

    /**
     * Returns the host, domain, IP of a URL so that it can be partitioned for politeness, depending
     * on the value of the config <i>partition.url.mode</i>.
     */
    public String getPartition(String url, Metadata metadata) {
        return getPartition(url, metadata, mode);
    }

    public void configure(Map<String, Object> stormConf) {

        mode =
                ConfUtils.getString(
                        stormConf,
                        Constants.PARTITION_MODEParamName,
                        Constants.PARTITION_MODE_HOST);

        // check that the mode is known
        if (!mode.equals(Constants.PARTITION_MODE_IP)
                && !mode.equals(Constants.PARTITION_MODE_DOMAIN)
                && !mode.equals(Constants.PARTITION_MODE_HOST)) {
            LOG.error("Unknown partition mode : {} - forcing to byHost", mode);
            mode = Constants.PARTITION_MODE_HOST;
        }

        LOG.info("Using partition mode : {}", mode);
    }
}
