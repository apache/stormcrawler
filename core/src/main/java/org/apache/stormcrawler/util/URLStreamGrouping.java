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

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Directs tuples to a specific bolt instance based on the URLPartitioner, e.g. byIP, byDomain or
 * byHost.
 *
 * <p>Use as follows with Flux :
 *
 * <pre>{@code
 * streams:
 *  - from: "spout"
 *    to: "status"
 *    grouping:
 *      type: CUSTOM
 *      customClass:
 *        className: "org.apache.stormcrawler.util.URLStreamGrouping"
 *        constructorArgs:
 *          - "byDomain"
 * }</pre>
 */
public class URLStreamGrouping implements CustomStreamGrouping, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(URLStreamGrouping.class);

    private List<Integer> targetTask;

    private URLPartitioner partitioner;

    private String partitionMode;

    /** Groups URLs based on the hostname. */
    public URLStreamGrouping() {}

    public URLStreamGrouping(String mode) {
        partitionMode = mode;
    }

    @Override
    public void prepare(
            WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTask = targetTasks;
        partitioner = new URLPartitioner();
        if (StringUtils.isNotBlank(partitionMode)) {
            Map<String, Object> conf = new HashMap<>();
            conf.put(Constants.PARTITION_MODEParamName, partitionMode);
            partitioner.configure(conf);
        }
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        // optimisation : single target
        if (targetTask.size() == 1) {
            return targetTask;
        }

        // missing content in tuple
        // should not happen
        // return empty task ids
        if (values.size() < 2) {
            LOG.error("Found tuple with less than 2 values. {}", values);
            return ImmutableList.of();
        }

        // the first value is always the URL
        // and the second the metadata
        String url = (String) values.get(0);
        Metadata metadata = (Metadata) values.get(1);
        String partitionKey = partitioner.getPartition(url, metadata);

        if (StringUtils.isBlank(partitionKey)) {
            LOG.error("No partition key for {}", url);
            return ImmutableList.of();
        }

        // hash on the key
        int partition = Math.abs(partitionKey.hashCode() % targetTask.size());
        return ImmutableList.of(targetTask.get(partition));
    }
}
