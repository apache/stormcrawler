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
import com.optimaize.langdetect.DetectedLanguage;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.RemoveMinorityScriptsTextFilter;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import com.optimaize.langdetect.text.TextObjectFactoryBuilder;
import com.optimaize.langdetect.text.UrlTextFilter;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseResult;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

/**
 * Language identification; the language codes gets stored in the metadata. <br>
 * To use it, just add the module as a dependency in your pom and include
 *
 * <p>```json { "class": "org.apache.stormcrawler.parse.filter.LanguageID", "name": "LanguageID",
 * "params": { "key": "lang" , "minProb": 0.99 , "extracted": "parse.lang"} } ```
 *
 * <p>in the parse filter config. Any value found in the metadata under the key specified by
 * _extracted_ will be normalised and stored in the metadata, otherwise the languages above the
 * probability will be used.
 */
public class LanguageID extends ParseFilter {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LanguageID.class);

    private static LanguageDetector languageDetector;

    private static final int maxTextLength = 10000;

    private static final TextObjectFactory textObjectFactory =
            new TextObjectFactoryBuilder()
                    .maxTextLength(maxTextLength)
                    .withTextFilter(UrlTextFilter.getInstance())
                    .withTextFilter(RemoveMinorityScriptsTextFilter.forThreshold(0.3))
                    .build();

    private String mdKey = "lang";
    private float minProb = 0.999f;
    private String extractedKeyName = "parse.lang";

    static {
        try {
            // load all languages:
            List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();
            // build language detector:
            languageDetector =
                    LanguageDetectorBuilder.create(NgramExtractors.standard())
                            .withProfiles(languageProfiles)
                            .build();
        } catch (IOException e) {
            throw new RuntimeException("Error while loading language profiles", e);
        }
    }

    @Override
    public void configure(@NotNull Map<String, Object> stormConf, @NotNull JsonNode filterParams) {
        JsonNode node = filterParams.get("key");
        if (node != null && node.isTextual()) {
            mdKey = node.asText("lang");
        }
        node = filterParams.get("minProb");
        if (node != null && node.isNumber()) {
            minProb = node.floatValue();
        }
        node = filterParams.get("extracted");
        if (node != null && node.isTextual()) {
            extractedKeyName = node.asText("parse.lang");
        }
    }

    @Override
    public void filter(String url, byte[] content, DocumentFragment doc, ParseResult parse) {

        // check whether the metadata already contains a lang value
        // in which case we normalise its value and use it
        Metadata m = parse.get(url).getMetadata();
        String extractedValue = m.getFirstValue(extractedKeyName);
        if (StringUtils.isNotBlank(extractedValue) && extractedValue.length() > 1) {
            extractedValue = extractedValue.substring(0, 2).toLowerCase(Locale.ENGLISH);
            LOG.info("Lang: {} extracted from page for {}", extractedValue, url);
            m.setValue(mdKey, extractedValue);
            return;
        }

        String text = parse.get(url).getText();
        if (StringUtils.isBlank(text)) {
            return;
        }

        if (text.length() > maxTextLength) {
            text = text.substring(0, maxTextLength);
        }

        TextObject textObject = textObjectFactory.forText(text);
        synchronized (languageDetector) {
            List<DetectedLanguage> probs = languageDetector.getProbabilities(textObject);
            if (probs == null || probs.size() == 0) {
                return;
            }
            for (DetectedLanguage lang : probs) {
                if (lang.getProbability() >= minProb) {
                    String code = lang.getLocale().getLanguage();
                    parse.get(url).getMetadata().addValue(mdKey, code);
                }
            }
        }
    }
}
