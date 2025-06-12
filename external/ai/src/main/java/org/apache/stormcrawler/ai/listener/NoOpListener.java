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
 * A no-op implementation of {@link LlmResponseListener} that ignores all responses and failures.
 *
 * <p>This is useful as a default listener when no specific behavior is needed for handling LLM
 * outputs or errors.
 */
public class NoOpListener implements LlmResponseListener {

    /**
     * Does nothing when a response is received.
     *
     * @param o the response object (ignored)
     */
    @Override
    public void onResponse(Object o) {
        // No-op
    }

    /**
     * Does nothing when a failure occurs.
     *
     * @param context the context of the failure (ignored)
     * @param e the exception thrown (ignored)
     */
    @Override
    public void onFailure(Object context, Exception e) {
        // No-op
    }
}
