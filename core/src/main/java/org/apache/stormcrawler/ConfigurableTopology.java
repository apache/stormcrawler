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

package org.apache.stormcrawler;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.ConfUtils;

public abstract class ConfigurableTopology {

    protected Config conf = new Config();

    public static void start(ConfigurableTopology topology, String[] args) {
        // loads the default configuration file
        Map<String, Object> defaultStormCrawlerConfig =
                Utils.findAndReadConfigFile("crawler-default.yaml", false);
        topology.conf.putAll(ConfUtils.extractConfigElement(defaultStormCrawlerConfig));

        String[] remainingArgs = topology.parse(args);
        topology.run(remainingArgs);
    }

    protected Config getConf() {
        return conf;
    }

    protected abstract int run(String[] args);

    /** Submits the topology with the name taken from the configuration. */
    protected int submit(Config conf, TopologyBuilder builder) {
        String name = ConfUtils.getString(conf, Config.TOPOLOGY_NAME);
        if (StringUtils.isBlank(name)) {
            throw new RuntimeException("No value found for " + Config.TOPOLOGY_NAME);
        }
        return submit(name, conf, builder);
    }

    /** Submits the topology under a specific name. */
    protected int submit(String name, Config conf, TopologyBuilder builder) {

        // register for serialization with Kryo
        Config.registerSerialization(conf, Metadata.class);
        Config.registerSerialization(conf, Status.class);

        try {
            StormSubmitter.submitTopology(name, conf, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    private String[] parse(String[] args) {

        List<String> newArgs = new ArrayList<>();
        Collections.addAll(newArgs, args);

        Iterator<String> iter = newArgs.iterator();
        while (iter.hasNext()) {
            String param = iter.next();
            if (param.equals("-conf")) {
                if (!iter.hasNext()) {
                    throw new RuntimeException("Conf file not specified");
                }
                iter.remove();
                String resource = iter.next();
                try {
                    ConfUtils.loadConf(resource, conf);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException("File not found : " + resource);
                }
                iter.remove();
            }
        }

        return newArgs.toArray(new String[0]);
    }
}
