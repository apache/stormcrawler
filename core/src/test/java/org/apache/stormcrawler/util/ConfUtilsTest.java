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

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConfUtilsTest {

    @Test
    void testIntWithOptional() throws MalformedURLException, IOException {
        Map<String, Object> conf = new java.util.HashMap<>();
        conf.put("prefix.suffix", 1);
        conf.put("prefix.optional.suffix", 2);
        int res = ConfUtils.getInt(conf, "prefix.", "optional.", "suffix", -1);
        org.junit.jupiter.api.Assertions.assertEquals(2, res);
        res = ConfUtils.getInt(conf, "prefix.", "missing.", "suffix", -1);
        org.junit.jupiter.api.Assertions.assertEquals(1, res);
        res = ConfUtils.getInt(conf, "totally.", "missing.", "inAction", -1);
        org.junit.jupiter.api.Assertions.assertEquals(-1, res);
    }

    @Test
    void testBooleanWithOptional() throws MalformedURLException, IOException {
        Map<String, Object> conf = new java.util.HashMap<>();
        conf.put("prefix.suffix", true);
        conf.put("prefix.optional.suffix", false);
        boolean res = ConfUtils.getBoolean(conf, "prefix.", "optional.", "suffix", true);
        Assertions.assertFalse(res);
        res = ConfUtils.getBoolean(conf, "prefix.", "missing.", "suffix", false);
        Assertions.assertTrue(res);
        res = ConfUtils.getBoolean(conf, "totally.", "missing.", "inAction", false);
        Assertions.assertFalse(res);
    }

    @Test
    void testStringWithOptional() throws MalformedURLException, IOException {
        Map<String, Object> conf = new java.util.HashMap<>();
        conf.put("prefix.suffix", "backup");
        conf.put("prefix.optional.suffix", "specific");
        String res = ConfUtils.getString(conf, "prefix.", "optional.", "suffix");
        Assertions.assertEquals("specific", res);
        res = ConfUtils.getString(conf, "prefix.", "missing.", "suffix");
        Assertions.assertEquals("backup", res);
        res = ConfUtils.getString(conf, "totally.", "missing.", "inAction", null);
        Assertions.assertNull(res);
        res = ConfUtils.getString(conf, "totally.", "missing.", "inAction");
        Assertions.assertNull(res);
    }
}
