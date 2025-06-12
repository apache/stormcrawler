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
package org.apache.stormcrawler.ai;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import org.apache.storm.Config;
import org.apache.stormcrawler.parse.TextExtractor;
import org.jsoup.parser.Parser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(
        named = "LLM_AVAILABLE",
        matches = ".*",
        disabledReason = "To run this test, there needs to be a running LLM instance available.")
public class OpenAITextExtractorTest {

    @Test
    void testExtraction() throws URISyntaxException, IOException {
        final Config conf = new Config();
        conf.put(OpenAITextExtractor.API_KEY, System.getProperty("OPENAI_API_KEY", ""));
        conf.put(
                OpenAITextExtractor.BASE_URL,
                System.getProperty("OPENAI_API_BASE_URL", "http://localhost:11434/v1"));
        conf.put(
                OpenAITextExtractor.MODEL_NAME,
                System.getProperty("OPENAI_API_MODEL_NAME", "gemma3:latest"));

        final TextExtractor textExtractor = new OpenAITextExtractor(conf);

        final String html =
                Files.readString(
                        Path.of(
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResource("stormcrawler.html")
                                        .toURI()),
                        StandardCharsets.UTF_8);
        assertNotNull(html);

        final String text = textExtractor.text(Parser.htmlParser().parseInput(html, "").body());

        final Pattern tagPattern =
                Pattern.compile(
                        "<\\s*/?\\s*(div|script|html|body|footer)\\b[^>]*>",
                        Pattern.CASE_INSENSITIVE);
        assertFalse(
                tagPattern.matcher(text).find(),
                "Extracted text should not contain disallowed HTML tags");
    }
}
