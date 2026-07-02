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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RefreshTagTest {

    final String[] htmlStrings =
            new String[] {
                "<html><head><META http-equiv=\"refresh\" content=\"0; URL=http://www.example.com/\"></head><body>Lorem ipsum.</body></html>",
                "<html><head><META http-equiv=\"refresh\" content=\"0;URL=http://www.example.com/\"></head><body>Lorem ipsum.</body></html>",
                "<html><head><meta http-equiv=\"refresh\" content=\"0;url='http://www.example.com/'\" \\><head></html>"
            };

    final String expected = "http://www.example.com/";

    @Test
    void testExtractRefreshURL() throws MalformedURLException, IOException {
        for (String htmlString : htmlStrings) {
            Document doc = Jsoup.parseBodyFragment(htmlString);
            String redirection = RefreshTag.extractRefreshURL(doc);
            Assertions.assertEquals(expected, redirection);
        }
    }
}
