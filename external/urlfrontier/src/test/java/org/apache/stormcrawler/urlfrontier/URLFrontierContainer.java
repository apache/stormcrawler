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

import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** A container for "crawlercommons/url-frontier". */
public class URLFrontierContainer extends GenericContainer<URLFrontierContainer>
        implements URLFrontierContainerConfig {
    public static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("crawlercommons/url-frontier");

    public static final int INTERNAL_FRONTIER_PORT = 7071;
    public static final int EXPOSED_TEST_FRONTIER_PORT = 33033;

    @Nullable private String rocksPath;
    private int prometheusPort = -1;

    public URLFrontierContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public URLFrontierContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
        addFixedExposedPort(EXPOSED_TEST_FRONTIER_PORT, INTERNAL_FRONTIER_PORT);
        setWaitStrategy(Wait.forLogMessage(".*Started URLFrontierServer.*\\n", 1));
    }

    @Override
    @Nullable
    public String getRocksPath() {
        return rocksPath;
    }

    @Override
    public void setRocksPath(@NotNull String rocksPath) {
        this.rocksPath = rocksPath;
    }

    @Override
    @Range(from = 0, to = 65535)
    public int getPrometheusPort() {
        return prometheusPort;
    }

    @Override
    public void setPrometheusPort(@Range(from = 0, to = 65535) int prometheusPort) {
        this.prometheusPort = prometheusPort;
    }

    public URLFrontierContainer withPrometheusPort(
            @Range(from = 0, to = 65535) int prometheusPort) {
        setPrometheusPort(prometheusPort);
        return this;
    }

    public URLFrontierContainer withRocksPath(@NotNull String rocksPath) {
        setRocksPath(rocksPath);
        return this;
    }

    @Override
    protected void configure() {
        ArrayList<String> args = new ArrayList<>(2);
        if (rocksPath != null) {
            args.add("rocksdb.path=" + rocksPath);
        }
        if (prometheusPort != -1) {
            setExposedPorts(List.of(INTERNAL_FRONTIER_PORT, prometheusPort));
            args.add("-s " + prometheusPort);
        }
        setCommandParts(args.toArray(new String[0]));
    }

    @NotNull
    public ConnectionInfo.URLFrontier getFrontierConnection() {
        return new ConnectionInfo.URLFrontier(getHost(), getMappedPort(INTERNAL_FRONTIER_PORT));
    }

    @Nullable
    public ConnectionInfo.Prometheus getPrometheusConnection() {
        if (prometheusPort == -1) {
            return null;
        }
        return new ConnectionInfo.Prometheus(getHost(), getMappedPort(prometheusPort));
    }

    public abstract static class ConnectionInfo {
        public abstract String getHost();

        public abstract int getPort();

        @NotNull
        public String getAddress() {
            return getHost() + ":" + getPort();
        }

        static final class URLFrontier extends ConnectionInfo {
            private final String host;
            private final int port;

            public URLFrontier(String host, int port) {
                this.host = host;
                this.port = port;
            }

            @Override
            public String getHost() {
                return host;
            }

            @Override
            public int getPort() {
                return port;
            }
        }

        static final class Prometheus extends ConnectionInfo {
            private final String host;
            private final int port;

            public Prometheus(String host, int port) {
                this.host = host;
                this.port = port;
            }

            @Override
            public String getHost() {
                return host;
            }

            @Override
            public int getPort() {
                return port;
            }
        }
    }
}
