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

package org.apache.stormcrawler.protocol.file;

import crawlercommons.robots.BaseRobotRules;
import java.io.File;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.Protocol;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.protocol.RobotRulesParser;
import org.apache.stormcrawler.util.ConfUtils;

public class FileProtocol implements Protocol {

    /**
     * Directory reads are confined to when set. Null or empty means the file scheme serves nothing:
     * a URL decides which path the worker opens, so reads must not be possible until an operator
     * has chosen a root on purpose.
     */
    public static final String ROOT_KEY = "file.protocol.root";

    private String encoding;

    private File root;

    @Override
    public void configure(Config conf) {
        encoding = ConfUtils.getString(conf, "file.encoding", "UTF-8");
        String rootPath = ConfUtils.getString(conf, ROOT_KEY, null);
        if (StringUtils.isNotBlank(rootPath)) {
            root = new File(rootPath);
            try {
                root = root.getCanonicalFile();
            } catch (IOException e) {
                throw new RuntimeException("Cannot resolve " + ROOT_KEY + ": " + rootPath, e);
            }
            if (!root.isDirectory()) {
                throw new RuntimeException(ROOT_KEY + " is not a directory: " + rootPath);
            }
        }
    }

    /** Returns the configured confinement root, or null when reads are disabled entirely. */
    File getRoot() {
        return root;
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, Metadata md) throws Exception {
        FileResponse response = new FileResponse(url, md, this);
        return response.toProtocolResponse();
    }

    @Override
    public BaseRobotRules getRobotRules(String url) {
        return RobotRulesParser.EMPTY_RULES;
    }

    public String getEncoding() {
        return encoding;
    }

    @Override
    public void cleanup() {}

    public static void main(String[] args) throws Exception {
        Protocol.main(new FileProtocol(), args);
    }
}
