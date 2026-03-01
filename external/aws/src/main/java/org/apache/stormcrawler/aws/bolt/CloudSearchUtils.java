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

package org.apache.stormcrawler.aws.bolt;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Hex;

public class CloudSearchUtils {

    private static MessageDigest digester;

    private static final Pattern INVALID_XML_CHARS =
            Pattern.compile("[^\\t\\n\\r -\\uD7FF\\uE000-\\uFFFD]");

    static {
        try {
            digester = MessageDigest.getInstance("SHA-512");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private CloudSearchUtils() {}

    /** Returns a normalised doc ID based on the URL of a document * */
    public static String getID(String url) {

        // the document needs an ID
        // see
        // http://docs.aws.amazon.com/cloudsearch/latest/developerguide/preparing-data.html#creating-document-batches
        // A unique ID for the document. A document ID can contain any
        // letter or number and the following characters: _ - = # ; : / ? @
        // &. Document IDs must be at least 1 and no more than 128
        // characters long.
        byte[] dig = digester.digest(url.getBytes(StandardCharsets.UTF_8));
        String ID = Hex.encodeHexString(dig);
        // is that even possible?
        if (ID.length() > 128) {
            throw new RuntimeException("ID larger than max 128 chars");
        }
        return ID;
    }

    public static String stripNonCharCodepoints(String input) {
        return INVALID_XML_CHARS.matcher(input).replaceAll("");
    }

    /**
     * Remove the non-cloudSearch-legal characters. Note that this might convert two fields to the
     * same name.
     *
     * @see <a
     *     href="http://docs.aws.amazon.com/cloudsearch/latest/developerguide/configuring-index-fields.html">
     *     configuring-index-fields.html</a>
     * @param name
     * @return
     */
    public static String cleanFieldName(String name) {
        String lowercase = name.toLowerCase(Locale.ROOT);
        lowercase = lowercase.replaceAll("[^a-z_0-9]", "_");
        if (lowercase.length() < 3 || lowercase.length() > 64) {
            throw new RuntimeException("Field name must be between 3 and 64 chars : " + lowercase);
        }
        if (lowercase.equals("score")) {
            throw new RuntimeException("Field name must be score");
        }
        return lowercase;
    }
}
