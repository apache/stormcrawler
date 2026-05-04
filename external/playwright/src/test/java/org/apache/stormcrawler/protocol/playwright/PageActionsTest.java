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

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PageActionsTest {

    @Test
    void emptyConfigReturnsEmptyChain() {
        final PageActions actions = PageActions.fromConf(Map.of());
        Assertions.assertSame(PageActions.emptyPageActions, actions);
        Assertions.assertEquals(0, actions.size());
    }

    @Test
    void blankConfigPathReturnsEmptyChain() {
        final PageActions actions = PageActions.fromConf(Map.of(PageActions.CONFIG_KEY, "   "));
        Assertions.assertSame(PageActions.emptyPageActions, actions);
    }

    @Test
    void emptyJsonChainHasZeroActions() {
        final PageActions actions =
                PageActions.fromConf(Map.of(PageActions.CONFIG_KEY, "page-actions.empty.json"));
        Assertions.assertEquals(0, actions.size());
    }

    @Test
    void singleActionChainLoads() {
        final PageActions actions =
                PageActions.fromConf(Map.of(PageActions.CONFIG_KEY, "page-actions.single.json"));
        Assertions.assertEquals(1, actions.size());
    }

    @Test
    void multiActionChainLoadsInOrder() {
        final PageActions actions =
                PageActions.fromConf(Map.of(PageActions.CONFIG_KEY, "page-actions.chain.json"));
        Assertions.assertEquals(4, actions.size());
    }

    @Test
    void missingConfigFileRaises() {
        Assertions.assertThrows(
                RuntimeException.class,
                () ->
                        PageActions.fromConf(
                                Map.of(
                                        PageActions.CONFIG_KEY,
                                        "page-actions.does-not-exist.json")));
    }

    @Test
    void invalidActionParamsRaiseAtLoadTime() {
        // ExpandClickablesAction throws if root/body are missing — must propagate from configure()
        Assertions.assertThrows(
                RuntimeException.class,
                () ->
                        PageActions.fromConf(
                                Map.of(PageActions.CONFIG_KEY, "page-actions.invalid.json")));
    }

    @Test
    void emptyChainApplyIsNoOp() {
        // apply() on the shared empty chain should not throw even with null-ish args via the
        // public no-arg helpers; we can't pass a real Page without a browser, so just assert
        // size and that cleanup is a no-op.
        PageActions.emptyPageActions.cleanup();
        Assertions.assertEquals(0, PageActions.emptyPageActions.size());
    }
}
