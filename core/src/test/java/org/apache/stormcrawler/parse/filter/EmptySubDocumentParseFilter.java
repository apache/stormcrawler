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

import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.w3c.dom.DocumentFragment;

/**
 * Creates an entry in the ParseResult for a URL which was never parsed and which carries no
 * content, no text and no metadata. Used to check that the parser bolts do not emit such entries as
 * documents.
 */
public class EmptySubDocumentParseFilter extends ParseFilter {

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {
        parse.getOrCreate(url + "#never-parsed");
    }
}
