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
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.ParseData;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.jetbrains.annotations.NotNull;
import org.w3c.dom.DocumentFragment;

/**
 * Computes a signature for a page, based on the binary content or text. If the content is empty,
 * the URL is used.
 *
 * <p>Configuration properties:
 *
 * <dl>
 *   <dt>useText
 *   <dd>compute signature on plain text, instead of binary content
 *   <dt>keyName
 *   <dd>name of the metadata field to hold the signature (default: &quot;signature&quot;)
 *   <dt>keyNameCopy
 *   <dd>name of the metadata field to hold a temporary copy of the signature used to decide by
 *       signature comparison whether the document has changed. If not defined or empty, the
 *       signature is not copied.
 * </dl>
 */
public class MD5SignatureParseFilter extends ParseFilter {

    private String keyName = "signature";

    private boolean useText = false;

    private String copyKeyName = null;

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {
        ParseData parseData = parse.get(url);
        Metadata metadata = parseData.getMetadata();
        if (copyKeyName != null) {
            String signature = metadata.getFirstValue(keyName);
            if (signature != null) {
                metadata.setValue(copyKeyName, signature);
            }
        }
        byte[] data = null;
        if (useText) {
            String text = parseData.getText();
            if (StringUtils.isNotBlank(text)) {
                data = text.getBytes(StandardCharsets.UTF_8);
            }
        } else {
            data = content;
        }
        if (data == null) {
            data = url.getBytes(StandardCharsets.UTF_8);
        }
        String hex = DigestUtils.md5Hex(data);
        metadata.setValue(keyName, hex);
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        JsonNode node = filterParams.get("useText");
        if (node != null && node.asBoolean()) {
            useText = true;
        }
        node = filterParams.get("keyName");
        if (node != null && node.isTextual()) {
            keyName = node.asText("signature");
        }
        node = filterParams.get("keyNameCopy");
        if (node != null && node.isTextual() && StringUtils.isNotBlank(node.asText(""))) {
            copyKeyName = node.asText("signatureOld");
        }
    }
}
