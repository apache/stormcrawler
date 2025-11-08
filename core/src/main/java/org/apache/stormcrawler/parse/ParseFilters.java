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

package org.apache.stormcrawler.parse;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import org.apache.stormcrawler.JSONResource;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.Configurable;
import org.jetbrains.annotations.NotNull;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

/**
 * Wrapper for the ParseFilters defined in a JSON configuration.
 *
 * @see Configurable#createConfiguredInstance(Class, Class, Map, JsonNode) for more information.
 */
public class ParseFilters extends ParseFilter implements JSONResource {

    public static final ParseFilters emptyParseFilter = new ParseFilters();

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ParseFilters.class);

    private ParseFilter[] filters;

    private ParseFilters() {
        filters = new ParseFilter[0];
    }

    private String configFile;

    private Map<String, Object> stormConf;

    /**
     * Loads and configure the ParseFilters based on the storm config if there is one otherwise
     * returns an emptyParseFilter.
     */
    public static ParseFilters fromConf(Map<String, Object> stormConf) {
        String parseconfigfile = ConfUtils.getString(stormConf, "parsefilters.config.file");
        if (StringUtils.isNotBlank(parseconfigfile)) {
            try {
                return new ParseFilters(stormConf, parseconfigfile);
            } catch (IOException e) {
                String message =
                        "Exception caught while loading the ParseFilters from " + parseconfigfile;
                LOG.error(message);
                throw new RuntimeException(message, e);
            }
        }

        return ParseFilters.emptyParseFilter;
    }

    /** loads the filters from a JSON configuration file. */
    public ParseFilters(Map<String, Object> stormConf, String configFile) throws IOException {
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
    public String getResourceFile() {
        return this.configFile;
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filtersConf) {
        List<ParseFilter> list =
                Configurable.createConfiguredInstance(
                        this.getClass(), ParseFilter.class, stormConf, filtersConf);
        filters = list.toArray(new ParseFilter[0]);
    }

    @Override
    public boolean needsDOM() {
        for (ParseFilter filter : filters) {
            boolean needsDom = filter.needsDOM();
            if (needsDom) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {

        for (ParseFilter filter : filters) {
            long start = System.currentTimeMillis();
            if (doc == null && filter.needsDOM()) {
                LOG.info(
                        "ParseFilter {} needs DOM but has none to work on - skip : {}",
                        filter.getClass().getName(),
                        url);
                continue;
            }
            filter.filter(url, content, doc, parse);
            long end = System.currentTimeMillis();
            LOG.debug("ParseFilter {} took {} msec", filter.getClass().getName(), end - start);
        }
    }

    @Override
    public void cleanup() {
        for (ParseFilter filter : filters) {
            filter.cleanup();
        }
    }

    /**
     * * Used for quick testing + debugging.
     *
     * @since 1.17
     */
    public static void main(String[] args) throws IOException, ParseException {

        Config conf = new Config();

        // loads the default configuration file
        Map<String, Object> defaultStormCrawlerConfig =
                Utils.findAndReadConfigFile("crawler-default.yaml", false);
        conf.putAll(ConfUtils.extractConfigElement(defaultStormCrawlerConfig));

        Options options = new Options();
        options.addOption("c", true, "stormcrawler configuration file");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("c")) {
            String confFile = cmd.getOptionValue("c");
            ConfUtils.loadConf(confFile, conf);
        }

        ParseFilters filters = ParseFilters.fromConf(conf);

        System.out.println(filters.filters.length + " filters found");

        ParseResult parse = new ParseResult();

        String url = cmd.getArgs()[0];

        byte[] content = IOUtils.toByteArray((new URL(url)).openStream());

        Document doc = Jsoup.parse(new String(content, StandardCharsets.UTF_8), url);

        filters.filter(url, content, DocumentFragmentBuilder.fromJsoup(doc), parse);

        System.out.println(parse.toString());

        System.exit(0);
    }
}
