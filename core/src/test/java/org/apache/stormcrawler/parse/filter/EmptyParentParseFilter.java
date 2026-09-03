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

import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.ParseData;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.w3c.dom.DocumentFragment;

/**
 * Empties the ParseData of the URL being parsed: no text, no content and no metadata. Used to check
 * that the parser bolts still emit the document itself in that case, so that its status keeps being
 * updated downstream.
 */
public class EmptyParentParseFilter extends ParseFilter {

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {
        ParseData parent = parse.getOrCreate(url);
        parent.setText(null);
        parent.setContent(new byte[0]);
        parent.setMetadata(new Metadata());
    }
}
