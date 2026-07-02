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

package org.apache.stormcrawler.warc;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.task.TopologyContext;

/**
 * From the WARC specs It is helpful to use practices within an institution that make it unlikely or
 * impossible to duplicate aggregate WARC file names. The convention used inside the Internet
 * Archive with ARC files is to name files according to the following pattern:
 * Prefix-Timestamp-Serial-Crawlhost.warc.gz Prefix is an abbreviation usually reflective of the
 * project or crawl that created this file. Timestamp is a 14- digit GMT timestamp indicating the
 * time the file was initially begun. Serial is an increasing serial-number within the process
 * creating the files, often (but not necessarily) unique with regard to the Prefix. Crawlhost is
 * the domain name or IP address of the machine creating the file.
 */
public class WARCFileNameFormat implements FileNameFormat {

    private int taskIndex;
    private String path = "/";
    private String prefix = "crawl";

    private final String extension = ".warc.gz";

    /**
     * Overrides the default prefix.
     *
     * @param prefix
     * @return
     */
    public FileNameFormat withPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public FileNameFormat withPath(String path) {
        this.path = path;
        return this;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext topologyContext) {
        this.taskIndex = topologyContext.getThisTaskIndex();
        int totalTasks =
                topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();
        // single task? let's not bother with the task index in the file name
        if (totalTasks == 1) {
            this.taskIndex = -1;
        }
    }

    @Override
    public String getName(long rotation, long timeStamp) {
        SimpleDateFormat fileDate = new SimpleDateFormat("yyyyMMddHHmmss", Locale.ROOT);
        fileDate.setTimeZone(TimeZone.getTimeZone("GMT"));
        String taskindexString = "";
        if (this.taskIndex != -1) {
            taskindexString = String.format(Locale.ROOT, "%02d", this.taskIndex) + "-";
        }
        return this.prefix
                + "-"
                + fileDate.format(new Date(timeStamp))
                + "-"
                + taskindexString
                + String.format(Locale.ROOT, "%05d", rotation)
                + this.extension;
    }

    public String getPath() {
        return this.path;
    }
}
