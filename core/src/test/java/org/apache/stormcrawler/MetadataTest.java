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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MetadataTest {

    @Test
    void testCopyWithPrefix() {
        Metadata metadata = new Metadata();
        metadata.addValue("fetch.statusCode", "500");
        metadata.addValue("fetch.error.count", "2");
        metadata.addValue("fetch.exception", "java.lang.Exception");
        metadata.addValue("fetchInterval", "200");
        metadata.addValue("isFeed", "true");
        metadata.addValue("depth", "1");
        Metadata copy = new Metadata();
        for (String key : metadata.keySet("fetch.")) {
            metadata.copy(copy, key);
        }
        Assertions.assertEquals(3, copy.size());
    }
}
