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

import org.apache.tika.parser.html.DefaultHtmlMapper;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.html.IdentityHtmlMapper;
import org.apache.tika.serialization.ComponentNameResolver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Checks which HtmlMapper the ParserBolt sends to the Tika Pipes fork. */
class ParserBoltPipesHtmlMapperTest {

    /** Registered by this module's META-INF/tika/parse-context.idx. */
    @Test
    void testIdentityMapperIsRegistered() {
        Assertions.assertTrue(ParserBolt.isIdentityMapperRegistered());
        Assertions.assertEquals(
                "identity-html-mapper",
                ComponentNameResolver.getFriendlyName(IdentityHtmlMapper.class));
    }

    @Test
    void testIdentityMapper() {
        Assertions.assertSame(
                IdentityHtmlMapper.INSTANCE,
                ParserBolt.pipesHtmlMapper(IdentityHtmlMapper.class, true));
    }

    /** Falls back to Tika's default, which the fork can always use, when not registered. */
    @Test
    void testIdentityMapperNotRegistered() {
        Assertions.assertNull(ParserBolt.pipesHtmlMapper(IdentityHtmlMapper.class, false));
    }

    @Test
    void testOtherMappers() {
        Assertions.assertNull(ParserBolt.pipesHtmlMapper(DefaultHtmlMapper.class, true));
        HtmlMapper custom = new DefaultHtmlMapper() {};
        Assertions.assertNull(ParserBolt.pipesHtmlMapper(custom.getClass(), true));
    }
}
