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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.stormcrawler.Metadata;

public class ParseResult implements Iterable<Map.Entry<String, ParseData>> {

    private List<Outlink> outlinks;
    private final Map<String, ParseData> parseMap;

    public ParseResult() {
        this(new HashMap<>(), new ArrayList<>());
    }

    public ParseResult(List<Outlink> links) {
        this(new HashMap<>(), links);
    }

    public ParseResult(Map<String, ParseData> map) {
        this(map, new ArrayList<>());
    }

    public ParseResult(Map<String, ParseData> map, List<Outlink> links) {
        if (map == null) {
            throw new NullPointerException();
        }
        parseMap = map;
        outlinks = links;
    }

    public boolean isEmpty() {
        return parseMap.isEmpty();
    }

    public int size() {
        return parseMap.size();
    }

    public List<Outlink> getOutlinks() {
        return outlinks;
    }

    public void setOutlinks(List<Outlink> outlinks) {
        this.outlinks = outlinks;
    }

    /**
     * Convenience accessor which returns the ParseData for the given URL, creating an empty entry
     * in this ParseResult if none can be found, useful to avoid unnecessary checks in the parse
     * plugins. Note that looking up a URL which has not been parsed modifies this ParseResult: the
     * entry created for it is treated like any other document and gets emitted by the parser bolts.
     *
     * @deprecated use {@link #getIfPresent(String)} for a read-only lookup or {@link
     *     #getOrCreate(String)} to create an entry, so that the intent is explicit at the call site
     * @return An existent instance of Parse for the given URL or an empty one if none can be found
     */
    @Deprecated
    public ParseData get(String url) {
        return getOrCreate(url);
    }

    /**
     * Returns the ParseData for the given URL without modifying this ParseResult: no entry is
     * created for URLs which were never parsed.
     *
     * @return the ParseData for the given URL or {@code null} if no entry exists for it
     */
    public ParseData getIfPresent(String url) {
        return parseMap.get(url);
    }

    /**
     * Returns the ParseData for the given URL, creating and storing an empty entry if none can be
     * found, so that modifications made to the returned instance are kept in this ParseResult.
     *
     * @return an existing or newly created instance of Parse for the given URL, never {@code null}
     */
    public ParseData getOrCreate(String url) {
        ParseData parse = parseMap.get(url);
        if (parse == null) {
            parse = new ParseData();
            parseMap.put(url, parse);
        }
        return parse;
    }

    /**
     * Returns the values for the given key on the ParseData for the given URL, if any.
     *
     * @return the values for the key or {@code null} if no entry exists for the URL or no value is
     *     stored for the key
     */
    public String[] getValues(String url, String key) {
        ParseData parseInfo = parseMap.get(url);
        if (parseInfo == null) {
            return null;
        }
        return parseInfo.getValues(key);
    }

    /** Add the key value to the metadata object for a given URL. */
    public void put(String url, String key, String value) {
        getOrCreate(url).getMetadata().addValue(key, value);
    }

    /** Set the metadata for a given URL. */
    public void set(String url, Metadata metadata) {
        getOrCreate(url).setMetadata(metadata);
    }

    public Map<String, ParseData> getParseMap() {
        return parseMap;
    }

    @Override
    public Iterator<Map.Entry<String, ParseData>> iterator() {
        return parseMap.entrySet().iterator();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("METADATA\n");

        parseMap.forEach(
                (k, v) ->
                        sb.append(k).append(": ").append(v.getMetadata().toString()).append("\n"));

        sb.append("\nOUTLINKS\n");

        outlinks.forEach(k -> sb.append(k.toString()).append("\n"));

        return sb.toString();
    }
}
