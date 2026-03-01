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

package org.apache.stormcrawler.protocol;

import java.io.FileNotFoundException;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.DelegatorProtocol.FilteredProtocol;
import org.apache.stormcrawler.util.ConfUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DelegationProtocolTest {

    private static final String OKHTTP = "org.apache.stormcrawler.protocol.okhttp.HttpProtocol";

    private static final String APACHE = "org.apache.stormcrawler.protocol.httpclient.HttpProtocol";

    @Test
    void getProtocolTest() throws FileNotFoundException {
        Config conf = new Config();
        ConfUtils.loadConf("src/test/resources/delegator-conf.yaml", conf);
        conf.put("http.agent.name", "this.is.only.a.test");
        DelegatorProtocol superProto = new DelegatorProtocol();
        superProto.configure(conf);
        // try single filter
        Metadata meta = new Metadata();
        meta.setValue("js", "true");
        FilteredProtocol pf = superProto.getProtocolFor("https://stormcrawler.apache.org", meta);
        Assertions.assertEquals("second", pf.id);
        // no filter at all
        meta = new Metadata();
        pf = superProto.getProtocolFor("https://www.example.com/robots.txt", meta);
        Assertions.assertEquals("default", pf.id);
        // should match the last instance
        // as the one above has more than one filter
        meta = new Metadata();
        meta.setValue("domain", "example.com");
        pf = superProto.getProtocolFor("https://example.com", meta);
        Assertions.assertEquals("default", pf.id);
        // everything should match
        meta = new Metadata();
        meta.setValue("test", "true");
        meta.setValue("depth", "3");
        meta.setValue("domain", "example.com");
        pf = superProto.getProtocolFor("https://www.example-two.com", meta);
        Assertions.assertEquals("first", pf.id);
        // should not match
        meta = new Metadata();
        meta.setValue("test", "false");
        meta.setValue("depth", "3");
        meta.setValue("domain", "example.com");
        pf = superProto.getProtocolFor("https://www.example-two.com", meta);
        // OR
        meta = new Metadata();
        meta.setValue("ping", null);
        pf = superProto.getProtocolFor("https://www.example-two.com", meta);
        // URLs
        meta = new Metadata();
        pf = superProto.getProtocolFor("https://www.example-two.com/large.pdf", meta);
        Assertions.assertEquals("fourth", pf.id);
        pf = superProto.getProtocolFor("https://www.example-two.com/large.doc", meta);
        Assertions.assertEquals("fourth", pf.id);
    }
}
