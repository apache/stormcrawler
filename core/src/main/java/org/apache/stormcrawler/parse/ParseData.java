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

package org.apache.stormcrawler.parse;

import org.apache.stormcrawler.Metadata;

public class ParseData {
    private byte[] content;
    private String text;
    private Metadata metadata;

    public ParseData() {
        this.metadata = new Metadata();
    }

    public ParseData(String text, Metadata metadata) {
        this.text = text;
        this.metadata = metadata;
        this.content = new byte[0];
    }

    public ParseData(Metadata md) {
        this.metadata = md;
    }

    public String getText() {
        return text;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public void put(String key, String value) {
        metadata.addValue(key, value);
    }

    public String get(String key) {
        return metadata.getFirstValue(key);
    }

    public String[] getValues(String key) {
        return metadata.getValues(key);
    }
}
