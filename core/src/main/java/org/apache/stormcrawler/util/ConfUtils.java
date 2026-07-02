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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.yaml.snakeyaml.Yaml;

public class ConfUtils {

    private ConfUtils() {}

    /**
     * Returns the value for prefix + optional + suffix. If nothing is found then return prefix +
     * suffix and if that fails too, the default value.
     */
    public static int getInt(
            final Map<String, Object> conf,
            final String prefix,
            final String optional,
            final String suffix,
            int defaultValue) {
        Object val = conf.get(prefix + optional + suffix);
        if (val != null) {
            return ((Number) val).intValue();
        }
        return getInt(conf, prefix + suffix, defaultValue);
    }

    public static int getInt(Map<String, Object> conf, String key, int defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return ((Number) ret).intValue();
    }

    public static long getLong(Map<String, Object> conf, String key, long defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return ((Number) ret).longValue();
    }

    public static float getFloat(Map<String, Object> conf, String key, float defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return ((Number) ret).floatValue();
    }

    /**
     * Returns the value for prefix + optional + suffix. If nothing is found then return prefix +
     * suffix and if that fails too, the default value.
     */
    public static boolean getBoolean(
            final Map<String, Object> conf,
            final String prefix,
            final String optional,
            final String suffix,
            boolean defaultValue) {
        Object ret = conf.get(prefix + optional + suffix);
        if (ret != null) {
            return (Boolean) ret;
        }
        return getBoolean(conf, prefix + suffix, defaultValue);
    }

    public static boolean getBoolean(Map<String, Object> conf, String key, boolean defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return (Boolean) ret;
    }

    /**
     * Returns the value for prefix + optional + suffix, if nothing is found then return prefix +
     * suffix or null.
     */
    public static String getString(
            final Map<String, Object> stormConf,
            final String prefix,
            final String optional,
            final String suffix) {
        String val = getString(stormConf, prefix + optional + suffix);
        if (val != null) {
            return val;
        }
        return getString(stormConf, prefix + suffix);
    }

    public static String getString(Map<String, Object> conf, String key) {
        return (String) conf.get(key);
    }

    /**
     * Returns the value for prefix + optional + suffix. If nothing is found then return prefix +
     * suffix and if that fails too, the default value.
     */
    public static String getString(
            final Map<String, Object> stormConf,
            final String prefix,
            final String optional,
            final String suffix,
            String defaultValue) {
        String val = getString(stormConf, prefix + optional + suffix);
        if (val != null) {
            return val;
        }
        val = getString(stormConf, prefix + suffix);
        if (val != null) {
            return val;
        }
        return defaultValue;
    }

    public static String getString(Map<String, Object> conf, String key, String defaultValue) {
        Object ret = conf.get(key);
        if (ret == null) {
            ret = defaultValue;
        }
        return (String) ret;
    }

    /**
     * Return one or more Strings regardless of whether they are represented as a single String or a
     * list in the config or an empty List if no value could be found for that key.
     */
    public static List<String> loadListFromConf(String paramKey, Map<String, Object> stormConf) {
        Object obj = stormConf.get(paramKey);
        List<String> list = new LinkedList<>();

        if (obj == null) {
            return list;
        }

        if (obj instanceof Collection) {
            list.addAll((Collection<String>) obj);
        } else { // single value?
            list.add(obj.toString());
        }
        return list;
    }

    /**
     * Return one or more Strings regardless of whether they are represented as a single String or a
     * list in the config for the combination all 2 String parameters. If nothing is found, try
     * using the prefix and suffix only to see if a more generic param was set e.g. "opensearch." +
     * "status." + "addresses" then "opensearch."+"addresses"
     *
     * @param prefix non-optional part of the key
     * @param optional string to be tried first
     * @param suffix non-optional part of the key
     * @return List of String values
     */
    public static List<String> loadListFromConf(
            final String prefix,
            final String optional,
            final String suffix,
            Map<String, Object> stormConf) {
        List<String> list = loadListFromConf(prefix + optional + suffix, stormConf);
        if (!list.isEmpty()) {
            return list;
        }
        return loadListFromConf(prefix + suffix, stormConf);
    }

    public static Config loadConf(String resource, Config conf) throws FileNotFoundException {
        Yaml yaml = new Yaml();
        Map<String, Object> ret =
                yaml.load(
                        new InputStreamReader(
                                new FileInputStream(resource), Charset.defaultCharset()));
        if (ret == null) {
            ret = new HashMap<>();
        } else {
            // contains a single config element ?
            ret = extractConfigElement(ret);
        }
        conf.putAll(ret);
        return conf;
    }

    /** If the config consists of a single key 'config', its values are used instead */
    public static Map<String, Object> extractConfigElement(Map<String, Object> conf) {
        if (conf.size() == 1) {
            Object confNode = conf.get("config");
            if (confNode != null && confNode instanceof Map) {
                conf = (Map<String, Object>) confNode;
            }
        }
        return conf;
    }
}
