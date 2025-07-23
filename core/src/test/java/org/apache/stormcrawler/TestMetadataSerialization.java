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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

class TestMetadataSerialization {

    @Test
    void testSerialization() throws IOException {
        Map<String, Object> conf = Utils.readDefaultConfig();
        Config.registerSerialization(conf, Metadata.class);
        KryoValuesSerializer kvs = new KryoValuesSerializer(conf);
        Metadata md = new Metadata();
        md.addValue("this_key", "has a value");
        // defensive lock
        md.lock();
        boolean exception = false;
        try {
            md.addValue("this_should", "fail");
        } catch (Exception e) {
            exception = true;
        }
        assertTrue(exception);
        byte[] content = kvs.serializeObject(md);
        KryoValuesDeserializer kvd = new KryoValuesDeserializer(conf);
        Metadata md2 = (Metadata) kvd.deserializeObject(content);
        // compare md1 and md2
        assertEquals(md.toString(), md2.toString());
    }
}
