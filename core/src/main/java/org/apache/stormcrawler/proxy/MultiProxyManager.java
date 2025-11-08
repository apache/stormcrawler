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

package org.apache.stormcrawler.proxy;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.httpclient.HttpProtocol;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.LoggerFactory;

/** MultiProxyManager is a ProxyManager implementation for a multiple proxy endpoints. */
public class MultiProxyManager implements ProxyManager {
    public enum ProxyRotation {
        RANDOM,
        LEAST_USED,
        ROUND_ROBIN,
    }

    protected Random rng;
    private SCProxy[] proxies;
    private ProxyRotation rotation;
    private final AtomicInteger lastAccessedIndex = new AtomicInteger(0);

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HttpProtocol.class);

    /** Default constructor for setting up the proxy manager. */
    private void init(ProxyRotation rotation) {
        // create rng with nano seed
        this.rng = new Random(System.nanoTime());
        // set rotation
        this.rotation = rotation;
    }

    public MultiProxyManager() {}

    @Override
    public void configure(Config conf) {
        // load proxy file from configuration
        String proxyFile = ConfUtils.getString(conf, "http.proxy.file", null);
        // load proxy rotation from config
        String proxyRot = ConfUtils.getString(conf, "http.proxy.rotation", "ROUND_ROBIN");

        // create variable to hold rotation scheme
        ProxyRotation proxyRotationScheme;

        // map rotation scheme to enum
        switch (proxyRot) {
            case "RANDOM":
                proxyRotationScheme = ProxyRotation.RANDOM;
                break;
            case "LEAST_USED":
                proxyRotationScheme = ProxyRotation.LEAST_USED;
                break;
            default:
                if (!proxyRot.equals("ROUND_ROBIN")) {
                    LOG.error(
                            "invalid proxy rotation scheme passed `{}` defaulting to ROUND_ROBIN; options: {}",
                            proxyRot,
                            ProxyRotation.values());
                }
                proxyRotationScheme = ProxyRotation.ROUND_ROBIN;
                break;
        }

        // call default constructor
        this.init(proxyRotationScheme);

        // create variable to hold file scanner
        Scanner scanner;

        // check if file exists in resources
        URL resourcesProxyFilePath = getClass().getClassLoader().getResource(proxyFile);

        // conditionally load file from resources
        if (resourcesProxyFilePath != null) {
            try {
                scanner = new Scanner(resourcesProxyFilePath.openStream(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException("failed to load proxy resource file: " + proxyFile, e);
            }
        } else {
            // open file to load proxies
            File proxyFileObj = new File(proxyFile);

            // create new scanner to read file line-by-line
            try {
                scanner = new Scanner(proxyFileObj, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException("failed to load proxy file: " + proxyFile, e);
            }
        }

        // create array to hold the loaded proxies
        SCProxy[] fileProxies = new SCProxy[0];

        // iterate over lines in file loading them into proxy objects
        while (scanner.hasNextLine()) {
            // read line containing proxy connection string
            String proxyConnectionString = scanner.nextLine();

            // skip commented lines and empty lines
            if (proxyConnectionString.startsWith("#")
                    || proxyConnectionString.startsWith("//")
                    || proxyConnectionString.isEmpty()
                    || proxyConnectionString.trim().isEmpty()) {
                continue;
            }

            // attempt to load proxy connection string and add proxy to proxies array
            fileProxies =
                    (SCProxy[]) ArrayUtils.add(fileProxies, new SCProxy(proxyConnectionString));
        }

        // close scanner
        scanner.close();

        // ensure that at least 1 proxy was loaded
        if (fileProxies.length < 1) {
            throw new IllegalArgumentException(
                    "at least one proxy must be loaded to create a multi-proxy manager");
        }

        // assign proxies to class variable
        this.proxies = fileProxies;
    }

    public void configure(ProxyRotation rotation, String[] proxyList) throws RuntimeException {
        // call default constructor
        this.init(rotation);

        // create array to hold the loaded proxies
        SCProxy[] fileProxies = new SCProxy[0];

        // iterate over proxy list loading each proxy into a native proxy object
        for (String proxyConnectionString : proxyList) {
            // attempt to load proxy connection string and add proxy to proxies array
            fileProxies =
                    (SCProxy[]) ArrayUtils.add(fileProxies, new SCProxy(proxyConnectionString));
        }

        // ensure that at least 1 proxy was loaded
        if (proxyList.length < 1) {
            throw new RuntimeException(
                    "at least one proxy must be passed to create a multi-proxy manager");
        }

        // assign proxies to class variable
        this.proxies = fileProxies;
    }

    private SCProxy getRandom() {
        // retrieve a proxy at random from the proxy array and return
        return this.proxies[this.rng.nextInt(this.proxies.length)];
    }

    private SCProxy getRoundRobin() {
        // ensure that last accessed does not exceed proxy list length
        if (this.lastAccessedIndex.get() >= this.proxies.length) {
            this.lastAccessedIndex.set(0);
        }

        // retrieve the current proxy, increment usage index, and return
        return this.proxies[this.lastAccessedIndex.getAndIncrement()];
    }

    private SCProxy getLeastUsed() {
        // start with index 0 in the proxy list
        SCProxy p = this.proxies[0];
        // save total usage to prevent cost of locking attribute on each call (this is a lazy
        // implementation for speed)
        int usage = p.getUsage();

        // iterate over proxies 1->END to find the proxy least used
        for (int i = 1; i < this.proxies.length; i++) {
            // retrieve usage of the proxy
            int u = this.proxies[i].getUsage();
            // check if this proxy was used less
            if (u < usage) {
                // set the new proxy
                p = this.proxies[i];
                usage = u;
            }
        }

        // return the least recently used proxy
        return p;
    }

    public int proxyCount() {
        return this.proxies.length;
    }

    @Override
    public Optional<SCProxy> getProxy(Metadata metadata) {
        // create a variable to hold the proxy generated in the following switch statement
        SCProxy proxy;

        // map the rotation algorithm to the correct function
        switch (this.rotation) {
            case ROUND_ROBIN:
                proxy = this.getRoundRobin();
                break;
            case LEAST_USED:
                proxy = this.getLeastUsed();
                break;
            default:
                proxy = this.getRandom();
        }

        // update usage for proxy
        proxy.incrementUsage();

        // return proxy
        return Optional.of(proxy);
    }
}
