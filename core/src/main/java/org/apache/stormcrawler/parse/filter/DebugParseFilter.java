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

package org.apache.stormcrawler.parse.filter;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.xml.serialize.XMLSerializer;
import org.jetbrains.annotations.NotNull;
import org.w3c.dom.DocumentFragment;

/** Dumps the DOM representation of a document into a file. */
public class DebugParseFilter extends ParseFilter {

    private OutputStream os;

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {

        try {
            XMLSerializer serializer = new XMLSerializer(os, null);
            serializer.serialize(doc);
            os.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        try {
            File outFile = Files.createTempFile("DOMDump", ".xml").toFile();
            os = FileUtils.openOutputStream(outFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean needsDOM() {
        return true;
    }
}
