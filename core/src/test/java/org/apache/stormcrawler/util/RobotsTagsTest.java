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

import java.net.MalformedURLException;
import org.apache.stormcrawler.Metadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RobotsTagsTest {

    @Test
    void testHTTPHeaders() throws MalformedURLException {
        Metadata md = new Metadata();
        RobotsTags tags = new RobotsTags(md, "");
        Assertions.assertFalse(tags.isNoCache());
        Assertions.assertFalse(tags.isNoFollow());
        Assertions.assertFalse(tags.isNoIndex());
        md = new Metadata();
        md.setValue("X-Robots-Tag", "none");
        tags = new RobotsTags(md, "");
        Assertions.assertTrue(tags.isNoCache());
        Assertions.assertTrue(tags.isNoFollow());
        Assertions.assertTrue(tags.isNoIndex());
        md = new Metadata();
        md.setValues("X-Robots-Tag", new String[] {"noindex", "nofollow"});
        tags = new RobotsTags(md, "");
        Assertions.assertFalse(tags.isNoCache());
        Assertions.assertTrue(tags.isNoFollow());
        Assertions.assertTrue(tags.isNoIndex());
        // expect the content to be incorrect
        md = new Metadata();
        md.setValue("X-Robots-Tag", "noindex, nofollow");
        tags = new RobotsTags(md, "");
        Assertions.assertFalse(tags.isNoCache());
        Assertions.assertTrue(tags.isNoFollow());
        Assertions.assertTrue(tags.isNoIndex());
    }
}
