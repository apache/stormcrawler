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

package org.apache.stormcrawler.filtering;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import org.apache.stormcrawler.JSONResource;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.Configurable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for the URLFilters defined in a JSON configuration.
 *
 * @see Configurable#createConfiguredInstance(Class, Class, Map, JsonNode) for more information.
 */
public class URLFilters extends URLFilter implements JSONResource {

    public static final URLFilters emptyURLFilters = new URLFilters();

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(URLFilters.class);

    private URLFilter[] filters;

    private URLFilters() {
        filters = new URLFilters[0];
    }

    private String configFile = "urlfilters.json";

    private Map<String, Object> stormConf;

    /**
     * Loads and configure the URLFilters based on the storm config if there is one otherwise
     * returns an empty URLFilter.
     */
    public static URLFilters fromConf(Map<String, Object> stormConf) {

        String configFile = ConfUtils.getString(stormConf, "urlfilters.config.file");
        if (StringUtils.isNotBlank(configFile)) {
            try {
                return new URLFilters(stormConf, configFile);
            } catch (IOException e) {
                String message = "Exception caught while loading the URLFilters from " + configFile;
                LOG.error(message);
                throw new RuntimeException(message, e);
            }
        }

        return URLFilters.emptyURLFilters;
    }

    /**
     * Loads the filters from a JSON configuration file.
     *
     * @throws IOException
     */
    public URLFilters(Map<String, Object> stormConf, String configFile) throws IOException {
        this.configFile = configFile;
        this.stormConf = stormConf;
        try {
            loadJSONResources();
        } catch (Exception e) {
            throw new IOException("Unable to build JSON object from file", e);
        }
    }

    @Override
    public void loadJSONResources(InputStream inputStream)
            throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode confNode = mapper.readValue(inputStream, JsonNode.class);
        configure(stormConf, confNode);
    }

    @Override
    public @Nullable String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter) {
        String normalizedUrl = urlToFilter;
        try {
            for (URLFilter filter : filters) {
                long start = System.currentTimeMillis();
                normalizedUrl = filter.filter(sourceUrl, sourceMetadata, normalizedUrl);
                long end = System.currentTimeMillis();
                LOG.debug("URLFilter {} took {} msec", filter.getClass().getName(), end - start);
                if (normalizedUrl == null) {
                    break;
                }
            }
        } catch (Exception e) {
            LOG.error("URL filtering threw exception", e);
        }
        return normalizedUrl;
    }

    @Override
    public String getResourceFile() {
        return this.configFile;
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filtersConf) {
        List<URLFilter> list =
                Configurable.createConfiguredInstance(
                        this.getClass(), URLFilter.class, stormConf, filtersConf);
        filters = list.toArray(new URLFilter[0]);
    }

    /** Utility to check the filtering of a URL. */
    public static void main(String[] args) throws ParseException {

        Config conf = new Config();

        // loads the default configuration file
        Map<String, Object> defaultStormCrawlerConfig =
                Utils.findAndReadConfigFile("crawler-default.yaml", false);
        conf.putAll(ConfUtils.extractConfigElement(defaultStormCrawlerConfig));

        String configFile = "urlfilters.json";

        Options options =
                new Options()
                        .addOption("f", true, "Filters configuration file. Default " + configFile);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("f")) {
            configFile = cmd.getOptionValue("f");
        }

        if (cmd.getArgList().isEmpty()) {
            System.err.println("Missing argument for input URL");
            System.exit(-1);
        }

        // read URL to check
        String inputUrl = cmd.getArgList().get(0);

        // if a URL has been specified in 2nd position
        String sourceUrl = inputUrl;
        if (cmd.getArgList().size() > 1) {
            sourceUrl = cmd.getArgList().get(1);
        }

        try {
            URLFilters filters = new URLFilters(conf, configFile);
            String normalizedUrl = inputUrl;
            try {
                for (URLFilter filter : filters.filters) {
                    long start = System.currentTimeMillis();
                    normalizedUrl =
                            filter.filter(new URL(sourceUrl), new Metadata(), normalizedUrl);
                    long end = System.currentTimeMillis();
                    System.out.println(
                            "\t["
                                    + filter.getClass().getName()
                                    + "] "
                                    + (end - start)
                                    + "msec => "
                                    + normalizedUrl);
                    if (normalizedUrl == null) {
                        break;
                    }
                }
            } catch (Exception e) {
                LOG.error("URL filtering threw exception", e);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        System.exit(0);
    }
}
