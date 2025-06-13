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

import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.stormcrawler.ai.listener.LlmResponseListener;
import org.apache.stormcrawler.ai.listener.NoOpListener;
import org.apache.stormcrawler.parse.TextExtractor;
import org.apache.stormcrawler.util.ConfUtils;
import org.jsoup.nodes.Element;

/**
 * Abstract base class for LLM-based text extractors that use a {@link ChatModel} to convert HTML
 * content into plain text. This class handles prompt preparation, configuration parsing, and model
 * interaction.
 *
 * <p>Subclasses must implement {@link #getChatModel(Map)} to provide the appropriate chat model
 * instance.
 */
public abstract class AbstractLLMTextExtractor implements TextExtractor {

    public static final String API_KEY = "textextractor.llm.api_key";
    public static final String BASE_URL = "textextractor.llm.url";
    public static final String MODEL_NAME = "textextractor.llm.model";
    public static final String SYSTEM_PROMPT = "textextractor.system.prompt";
    public static final String USER_PROMPT = "textextractor.llm.prompt";
    public static final String USER_REQUEST = "textextractor.llm.user_request";
    public static final String LISTENER_CLASS = "textextractor.llm.listener.clazz";

    private final ChatModel model;
    private final SystemMessage systemMessage;
    private final String userMessage;
    private final String userRequest;
    private final LlmResponseListener listener;

    /**
     * Constructs the extractor using the given configuration. Initializes the chat model,
     * system/user prompts, user request, and listener.
     *
     * @param stormConf configuration map with model and prompt settings
     */
    public AbstractLLMTextExtractor(Map<String, Object> stormConf) {
        this.model = getChatModel(stormConf);
        this.systemMessage =
                SystemMessage.from(
                        ConfUtils.getString(
                                stormConf,
                                SYSTEM_PROMPT,
                                "You are an expert in extracting content from plain HTML input."));
        this.userMessage =
                ConfUtils.getString(
                        stormConf, USER_PROMPT, readFromClasspath("llm-default-prompt.txt"));
        this.userRequest = ConfUtils.getString(stormConf, USER_REQUEST, "");
        final String clazz =
                ConfUtils.getString(stormConf, LISTENER_CLASS, NoOpListener.class.getName());
        try {
            listener =
                    (LlmResponseListener)
                            Class.forName(clazz).getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException
                | InvocationTargetException
                | InstantiationException
                | IllegalAccessException
                | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Implemented by subclasses to return a specific {@link ChatModel} instance.
     *
     * @param stormConf the configuration map
     * @return the chat model to be used
     */
    protected abstract ChatModel getChatModel(Map<String, Object> stormConf);

    /**
     * Reads a file from the classpath and returns its content as a UTF-8 string.
     *
     * @param resource the name of the resource to read
     * @return the content of the resource file
     * @throws RuntimeException if the resource is not found or cannot be read
     */
    protected String readFromClasspath(String resource) {
        try {
            final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            try (InputStream is = classLoader.getResourceAsStream(resource)) {
                if (is == null) {
                    throw new FileNotFoundException("Resource not found: " + resource);
                }
                return new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Extracts text from a given JSoup {@link Element} by sending a prompt to the LLM model.
     *
     * @param element an {@link Element} representing a portion of HTML
     * @return the LLM-extracted plain text or an empty string on failure
     */
    @Override
    public String text(Object element) {
        if (element instanceof Element e) {
            try {
                final ChatRequest chatRequest =
                        ChatRequest.builder()
                                .messages(
                                        systemMessage,
                                        UserMessage.from(
                                                replacePlaceholders(userMessage, e.html())))
                                .build();
                final ChatResponse response = model.chat(chatRequest);
                listener.onResponse(response);
                return response.aiMessage().text();
            } catch (RuntimeException ex) {
                listener.onFailure(element, ex);
            }
        }
        return "";
    }

    /**
     * Replaces placeholders in the user message template with the actual HTML content and user
     * request.
     *
     * @param userMessage the original user message template
     * @param html the HTML string to insert
     * @return the updated user message string with placeholders replaced
     */
    protected String replacePlaceholders(String userMessage, String html) {
        userMessage = userMessage.replace("{HTML}", html);
        userMessage = userMessage.replace("{REQUEST}", userRequest);
        return userMessage;
    }
}
