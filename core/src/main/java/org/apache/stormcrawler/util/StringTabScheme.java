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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;

/** Converts a byte array into URL + metadata. */
public class StringTabScheme implements Scheme {

    @Override
    public List<Object> deserialize(ByteBuffer bytes) {
        String input = new String(bytes.array(), StandardCharsets.UTF_8);

        String[] tokens = input.split("\t");
        if (tokens.length < 1) {
            return new Values();
        }

        String url = tokens[0];
        Metadata metadata = null;

        for (int i = 1; i < tokens.length; i++) {
            String token = tokens[i];
            // split into key & value
            int firstequals = token.indexOf("=");
            String value = null;
            String key = token;
            if (firstequals != -1) {
                key = token.substring(0, firstequals);
                value = token.substring(firstequals + 1);
            }
            if (metadata == null) {
                metadata = new Metadata();
            }
            metadata.addValue(key, value);
        }

        if (metadata == null) {
            metadata = new Metadata();
        }

        return new Values(url, metadata);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("url", "metadata");
    }
}
