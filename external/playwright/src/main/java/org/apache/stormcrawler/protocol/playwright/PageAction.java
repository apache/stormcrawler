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

package org.apache.stormcrawler.protocol.playwright;

import com.microsoft.playwright.Page;
import org.apache.storm.task.IBolt;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.AbstractConfigurable;
import org.jetbrains.annotations.NotNull;

/**
 * A pluggable post-navigate page transformation. Each implementation is invoked after {@code
 * page.navigate()} succeeds and before {@code page.content()} is captured, so any DOM mutations it
 * makes are reflected in the rendered content returned by the protocol.
 *
 * <p>Actions are loaded as an ordered chain via {@link PageActions} from a JSON file referenced by
 * the {@code playwright.page.actions.config.file} configuration key. They follow the same {@link
 * org.apache.stormcrawler.util.Configurable} pattern as URL/parse filters.
 */
public abstract class PageAction extends AbstractConfigurable {

    /**
     * Apply this action to the page.
     *
     * @param page the live Playwright {@link Page}, already navigated to {@code url}
     * @param url the URL being fetched
     * @param sourceMetadata input metadata associated with the URL (read-only intent)
     * @param responseMetadata response metadata being built up; actions may add diagnostics here
     */
    public abstract void apply(
            @NotNull final Page page,
            @NotNull final String url,
            @NotNull final Metadata sourceMetadata,
            @NotNull final Metadata responseMetadata)
            throws Exception;

    /** Release any resources held by the action. See {@link IBolt#cleanup()} for more details. */
    public void cleanup() {
        // nothing to do here
    }
}
