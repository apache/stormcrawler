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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.LoggerFactory;

public class TestOutputCollector implements IOutputCollector, ISpoutOutputCollector {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TestOutputCollector.class);

    private List<Tuple> acked = new ArrayList<>();
    private List<Tuple> failed = new ArrayList<>();
    private Map<String, List<List<Object>>> emitted = new HashMap<>();

    @Override
    public void reportError(Throwable error) {
        LOG.error("Got exception", error);
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuples) {
        addEmittedTuple(streamId, tuples);
        // No idea what to return
        return null;
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        addEmittedTuple(streamId, tuple);
        return null;
    }

    @Override
    public void emitDirect(
            int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {}

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        addEmittedTuple(streamId, tuple);
    }

    @Override
    public void ack(Tuple input) {
        acked.add(input);
    }

    @Override
    public void fail(Tuple input) {
        failed.add(input);
    }

    public List<List<Object>> getEmitted() {
        return getEmitted(Utils.DEFAULT_STREAM_ID);
    }

    public List<List<Object>> getEmitted(String streamId) {
        return emitted.getOrDefault(streamId, Collections.emptyList());
    }

    public List<Tuple> getAckedTuples() {
        return acked;
    }

    public List<Tuple> getFailedTuples() {
        return failed;
    }

    private void addEmittedTuple(String streamId, List<Object> tuple) {
        emitted.computeIfAbsent(streamId, k -> new ArrayList<>()).add(tuple);
    }

    @Override
    public long getPendingCount() {
        return 0;
    }

    @Override
    public void resetTimeout(Tuple tuple) {}

    @Override
    public void flush() {}
}
