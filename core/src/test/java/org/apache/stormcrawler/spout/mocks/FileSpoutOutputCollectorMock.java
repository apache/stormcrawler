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

package org.apache.stormcrawler.spout.mocks;

import java.util.List;
import org.apache.storm.spout.SpoutOutputCollector;

/**
 * This is just a stub implementation to be able to test {@link
 * org.apache.stormcrawler.spout.FileSpout}
 */
public class FileSpoutOutputCollectorMock extends SpoutOutputCollector {

    private String streamId;
    private List<Object> tuple;
    private Object messageId;

    public FileSpoutOutputCollectorMock() {
        super(null);
    }

    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        this.streamId = streamId;
        this.tuple = tuple;
        this.messageId = messageId;
        return List.of();
    }

    public List<Integer> emit(List<Object> tuple, Object messageId) {
        return emit("default", tuple, messageId);
    }

    public List<Integer> emit(List<Object> tuple) {
        return this.emit(tuple, null);
    }

    public List<Integer> emit(String streamId, List<Object> tuple) {
        return this.emit(streamId, tuple, null);
    }

    public String getStreamId() {
        return streamId;
    }

    public List<Object> getTuple() {
        return tuple;
    }

    public Object getMessageId() {
        return messageId;
    }
}
