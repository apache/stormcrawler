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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.LoggerFactory;

/**
 * Proxy class is used as the central interface to proxy based interactions with a single remote
 * server The class stores all information relating to the remote server, authentication, and usage
 * activity.
 */
public class SCProxy {
    // define regex expression used to parse connection strings
    private static final Pattern PROXY_STRING_REGEX =
            Pattern.compile(
                    "(?<protocol>[^:]+)://(?:(?<username>[^:]+):(?<password>[^:]+)@)?(?<host>[^@:]+):(?<port>\\d{2,5})");

    // create logger
    protected static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SCProxy.class);

    // define fields for basic information
    private final String protocol;
    private final String address;
    private final String port;
    private String username;
    private String password;
    private String country;
    private String area;
    private String location;
    private String status;

    // define fields for management
    private AtomicInteger totalUsage;

    /** Default constructor for setting up the proxy. */
    private void init() {
        // initialize usage tracker to 0
        this.totalUsage = new AtomicInteger();
    }

    /** Construct a proxy object from a valid proxy connection string. */
    public SCProxy(String connectionString) throws IllegalArgumentException {
        // call default constructor
        this.init();

        // load connection string into regex matched
        Matcher matcher = PROXY_STRING_REGEX.matcher(connectionString);

        // ensure that connection string is a valid proxy
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "passed connection string is not of valid proxy format "
                            + "(<PROTO>://(<USER>:<PASS>@)<HOST>:<PORT>) : "
                            + connectionString);
        }

        // load required parameters
        this.protocol = matcher.group("protocol");
        this.address = matcher.group("host");
        this.port = matcher.group("port");

        // load optional authentication data
        try {
            this.username = matcher.group("username");
            this.password = matcher.group("password");
        } catch (IllegalArgumentException ignored) {
        }
    }

    /** Construct a proxy class from it's variables. */
    public SCProxy(
            String protocol,
            String address,
            String port,
            String username,
            String password,
            String country,
            String area,
            String location,
            String status)
            throws IllegalArgumentException {
        // call default constructor
        this.init();

        // load required parameters
        this.protocol = protocol;
        this.address = address;
        this.port = port;

        // load optional parameters
        if (!username.isBlank()) {
            this.username = username;
        }
        if (!password.isBlank()) {
            this.password = password;
        }
        if (!country.isBlank()) {
            this.country = country;
        }
        if (!area.isBlank()) {
            this.area = area;
        }
        if (!location.isBlank()) {
            this.location = location;
        }
        if (!status.isBlank()) {
            this.status = status;
        }
    }

    /** Formats the proxy information into a URL compatible connection string. */
    public String toString() {
        // assemble base string with address and password
        String proxyString = this.address + ":" + this.port;

        // conditionally add authentication details
        if (this.username != null && this.password != null) {
            // re-assemble url with auth details prepended
            proxyString = this.username + ":" + this.password + "@" + proxyString;
        }

        // prepend protocol string to url and return
        return this.protocol + "://" + proxyString;
    }

    /** Increments the usage tracker for the proxy. */
    public void incrementUsage() {
        this.totalUsage.incrementAndGet();
    }

    /** Retrieves the current usage of the proxy. */
    public int getUsage() {
        return this.totalUsage.get();
    }

    public String getProtocol() {
        return protocol;
    }

    public String getAddress() {
        return address;
    }

    public String getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getCountry() {
        return country;
    }

    public String getArea() {
        return area;
    }

    public String getLocation() {
        return location;
    }

    public String getStatus() {
        return status;
    }
}
