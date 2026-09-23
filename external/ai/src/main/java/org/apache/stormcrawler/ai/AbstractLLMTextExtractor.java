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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.stormcrawler.ai.listener.LlmResponseListener;
import org.apache.stormcrawler.ai.listener.NoOpListener;
import org.apache.stormcrawler.parse.TextExtractor;
import org.apache.stormcrawler.util.ConfUtils;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.TextNode;
import org.jsoup.parser.Parser;

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
    public static final String TEXT_MAX_LENGTH = "textextractor.llm.text.maxlength";

    /** marker tokens such as {@code <|HTML_CONTENT_END|>} used to delimit sections of a prompt */
    private static final Pattern MARKER_PATTERN = Pattern.compile("<\\|[^|<>\\s]+\\|>");

    private static final String CONTENT_START = "<content>";
    private static final String CONTENT_END = "</content>";

    private final ChatModel model;
    private final SystemMessage systemMessage;
    private final String userMessage;
    private final String userRequest;
    private final Set<String> markers;
    private final int textMaxLength;
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
        this.markers = findMarkers(userMessage);
        this.textMaxLength = ConfUtils.getInt(stormConf, TEXT_MAX_LENGTH, -1);
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
     * <p>The reply is reduced to the content of its {@code <content>} envelope when it has one, any
     * markup left in it is removed and it is truncated to the length set with {@value
     * #TEXT_MAX_LENGTH}, if any.
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
                return cleanReply(response.aiMessage().text());
            } catch (RuntimeException ex) {
                listener.onFailure(element, ex);
            }
        }
        return "";
    }

    /**
     * Replaces placeholders in the user message template with the actual HTML content and user
     * request. Marker tokens of the template, such as {@code <|HTML_CONTENT_END|>}, are removed
     * from the HTML first so that the page cannot close or open a section of the prompt.
     *
     * @param userMessage the original user message template
     * @param html the HTML string to insert
     * @return the updated user message string with placeholders replaced
     */
    protected String replacePlaceholders(String userMessage, String html) {
        // the request is substituted first so that a {REQUEST} in the page is left as it is
        userMessage = userMessage.replace("{REQUEST}", userRequest);
        return userMessage.replace("{HTML}", removeMarkers(html));
    }

    /**
     * Returns the text of a reply: the content of its {@code <content>} envelope if there is one,
     * or the whole reply otherwise, without markup and truncated to the length set with {@value
     * #TEXT_MAX_LENGTH}.
     *
     * @param reply the text returned by the model
     * @return the extracted text
     */
    protected String cleanReply(String reply) {
        if (reply == null) {
            return "";
        }
        final int start = reply.indexOf(CONTENT_START);
        if (start >= 0) {
            final int end = reply.lastIndexOf(CONTENT_END);
            final int from = start + CONTENT_START.length();
            reply = end >= from ? reply.substring(from, end) : reply.substring(from);
        }
        String text = stripMarkup(reply).strip();
        if (textMaxLength >= 0 && text.length() > textMaxLength) {
            int cut = textMaxLength;
            if (cut > 0 && Character.isHighSurrogate(text.charAt(cut - 1))) {
                cut--;
            }
            text = text.substring(0, cut);
        }
        return text;
    }

    /** keeps the text nodes of the input and their line breaks, dropping elements and comments */
    private static String stripMarkup(String text) {
        final StringBuilder sb = new StringBuilder(text.length());
        Parser.htmlParser()
                .parseInput(text, "")
                .body()
                .traverse(
                        (node, depth) -> {
                            if (node instanceof TextNode t) {
                                sb.append(t.getWholeText());
                            }
                        });
        return sb.toString();
    }

    private String removeMarkers(String html) {
        if (markers.isEmpty()) {
            return html;
        }
        // repeat, as removing one marker can join the pieces of another
        String previous;
        do {
            previous = html;
            for (String marker : markers) {
                html = html.replace(marker, "");
            }
        } while (!html.equals(previous));
        return html;
    }

    private static Set<String> findMarkers(String template) {
        final Set<String> found = new LinkedHashSet<>();
        final Matcher m = MARKER_PATTERN.matcher(template);
        while (m.find()) {
            found.add(m.group());
        }
        return found;
    }
}
