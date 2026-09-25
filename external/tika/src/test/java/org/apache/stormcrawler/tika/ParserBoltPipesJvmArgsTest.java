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

package org.apache.stormcrawler.tika;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ParserBoltPipesJvmArgsTest {

    @Test
    void defaultHeapWhenNoArgumentIsSet() {
        Assertions.assertEquals(List.of("-Xmx512m"), ParserBolt.forkedJvmArgs(Map.of()));
    }

    @Test
    void defaultHeapIsAddedToOtherArguments() {
        Assertions.assertEquals(
                List.of("-Djava.awt.headless=true", "-Xmx512m"),
                ParserBolt.forkedJvmArgs(
                        Map.of(ParserBolt.PIPES_JVM_ARGS_PARAM, "-Djava.awt.headless=true")));
    }

    @Test
    void configuredHeapIsKept() {
        Assertions.assertEquals(
                List.of("-Xmx1g"),
                ParserBolt.forkedJvmArgs(Map.of(ParserBolt.PIPES_JVM_ARGS_PARAM, "-Xmx1g")));
    }

    @Test
    void configuredMaxHeapSizeIsKept() {
        Assertions.assertEquals(
                List.of("-XX:MaxHeapSize=2g"),
                ParserBolt.forkedJvmArgs(
                        Map.of(ParserBolt.PIPES_JVM_ARGS_PARAM, "-XX:MaxHeapSize=2g")));
    }

    @Test
    void configuredRamPercentageIsKept() {
        Assertions.assertEquals(
                List.of("-XX:MaxRAMPercentage=25"),
                ParserBolt.forkedJvmArgs(
                        Map.of(
                                ParserBolt.PIPES_JVM_ARGS_PARAM,
                                List.of("-XX:MaxRAMPercentage=25"))));
    }
}
