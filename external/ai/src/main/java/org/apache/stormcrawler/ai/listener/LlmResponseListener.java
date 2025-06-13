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
package org.apache.stormcrawler.ai.listener;

/**
 * A listener interface for handling responses and failures from LLM-based operations.
 *
 * <p>This interface allows implementers to process successful responses or handle errors that occur
 * during interaction with a large language model (LLM).
 */
public interface LlmResponseListener {

    /**
     * Invoked when a response is successfully received from the LLM.
     *
     * @param o the response object (typically a {@link
     *     dev.langchain4j.model.chat.response.ChatResponse})
     */
    void onResponse(Object o);

    /**
     * Invoked when an error occurs during LLM interaction.
     *
     * @param context an optional context object related to the failure, may be {@code null}
     * @param e the exception that was thrown
     */
    void onFailure(Object context, Exception e);
}
