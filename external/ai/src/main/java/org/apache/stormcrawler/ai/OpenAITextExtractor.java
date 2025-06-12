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

import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.stormcrawler.parse.TextExtractor;
import org.apache.stormcrawler.util.ConfUtils;
import org.jsoup.parser.Parser;

/**
 * A concrete implementation of {@link AbstractLLMTextExtractor} that uses the OpenAI-compatible
 * model configuration to extract meaningful text from HTML content.
 *
 * <p>This class retrieves its configuration from a Storm configuration map and initializes an
 * {@link OpenAiChatModel} accordingly. It can be used as a command-line tool to extract text from a
 * given HTML file.
 */
public class OpenAITextExtractor extends AbstractLLMTextExtractor implements TextExtractor {

    /**
     * Constructs a new {@code OpenAITextExtractor} using the provided Storm configuration.
     *
     * @param stormConf a map containing the configuration parameters
     */
    public OpenAITextExtractor(Map<String, Object> stormConf) {
        super(stormConf);
    }

    /**
     * Builds and returns an {@link OpenAiChatModel} using parameters from the configuration.
     *
     * @param stormConf the configuration map containing model parameters
     * @return an initialized {@link ChatModel} implementation
     */
    @Override
    protected ChatModel getChatModel(Map<String, Object> stormConf) {
        return OpenAiChatModel.builder()
                .apiKey(ConfUtils.getString(stormConf, API_KEY))
                .baseUrl(ConfUtils.getString(stormConf, BASE_URL))
                .modelName(ConfUtils.getString(stormConf, MODEL_NAME))
                .build();
    }

    /**
     * Command-line entry point for testing the extractor locally.
     *
     * <p>Usage: {@code java OpenAITextExtractor config.yaml input.html}
     *
     * @param args command-line arguments: path to the config file and the HTML file
     * @throws IOException if there is an error reading the configuration or HTML file
     */
    public static void main(String[] args) throws IOException {
        final Map<String, Object> conf = ConfUtils.loadConf(args[0], new Config());
        final OpenAITextExtractor textExtractor = new OpenAITextExtractor(conf);

        final String html = Files.readString(Path.of(args[1]), StandardCharsets.UTF_8);

        final String text = textExtractor.text(Parser.htmlParser().parseInput(html, "").body());

        System.out.println(text);
    }
}
