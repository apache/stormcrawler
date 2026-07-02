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

import crawlercommons.robots.BaseRobotRules;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.StringTabScheme;

public interface Protocol {

    void configure(Config conf);

    /**
     * Fetches the content and additional metadata
     *
     * <p>IMPORTANT: the metadata returned within the response should only be new <i>additional</i>,
     * no need to return the metadata passed in.
     *
     * @param url the location of the content
     * @param metadata extra information
     * @return the content and optional metadata fetched via this protocol
     */
    ProtocolResponse getProtocolOutput(String url, Metadata metadata) throws Exception;

    BaseRobotRules getRobotRules(String url);

    void cleanup();

    public static void main(Protocol protocol, String[] args) throws Exception {
        Config conf = new Config();

        // loads the default configuration file
        Map<String, Object> defaultStormCrawlerConfig =
                Utils.findAndReadConfigFile("crawler-default.yaml", false);
        conf.putAll(ConfUtils.extractConfigElement(defaultStormCrawlerConfig));

        Options options = new Options();
        options.addOption("f", true, "configuration file");
        options.addOption("b", false, "dump binary content to temp file");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String confFile = cmd.getOptionValue("f");
        if (confFile != null) {
            ConfUtils.loadConf(confFile, conf);
        }

        boolean binary = cmd.hasOption("b");

        protocol.configure(conf);

        Set<Runnable> threads = new HashSet<>();

        class Fetchable implements Runnable {
            final String url;
            final Metadata md;

            Fetchable(String line) {
                StringTabScheme scheme = new StringTabScheme();
                List<Object> tuple =
                        scheme.deserialize(ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8)));
                this.url = (String) tuple.get(0);
                this.md = (Metadata) tuple.get(1);
            }

            public void run() {

                final StringBuilder stringB = new StringBuilder();
                stringB.append(url).append("\n");

                final boolean skipRobots =
                        ConfUtils.getBoolean(conf, "http.robots.file.skip", false);

                if (!skipRobots) {
                    BaseRobotRules rules = protocol.getRobotRules(url);
                    stringB.append("robots allowed: ").append(rules.isAllowed(url)).append("\n");
                    if (rules instanceof RobotRules) {
                        stringB.append("robots requests: ")
                                .append(((RobotRules) rules).getContentLengthFetched().length)
                                .append("\n");
                    }
                    stringB.append("sitemaps identified: ")
                            .append(rules.getSitemaps().size())
                            .append("\n");
                }

                long start = System.currentTimeMillis();
                ProtocolResponse response;
                try {
                    response = protocol.getProtocolOutput(url, md);
                    stringB.append(response.getMetadata()).append("\n");
                    stringB.append("status code: ").append(response.getStatusCode()).append("\n");
                    stringB.append("content length: ")
                            .append(response.getContent().length)
                            .append("\n");
                    long timeFetching = System.currentTimeMillis() - start;
                    stringB.append("fetched in : ").append(timeFetching).append(" msec\n");

                    if (binary) {
                        Path p = Files.createTempFile("sc-protocol-", ".dump");
                        FileUtils.writeByteArrayToFile(p.toFile(), response.getContent());
                        stringB.append("dumped content to : ").append(p);
                    }

                    System.out.println(stringB);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    threads.remove(this);
                }
            }
        }

        for (String arg : cmd.getArgs()) {
            Fetchable p = new Fetchable(arg);
            threads.add(p);
            new Thread(p).start();
        }

        while (threads.size() > 0) {
            Thread.sleep(1000);
        }

        protocol.cleanup();
        System.exit(0);
    }
}
