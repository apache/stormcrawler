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

package org.apache.stormcrawler.filtering;

import java.net.URL;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.util.AbstractConfigurable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Unlike Nutch, URLFilters can normalise the URLs as well as filtering them. URLFilter instances
 * should be used via {@link URLFilters}
 *
 * @see URLFilters for more information.
 */
public abstract class URLFilter extends AbstractConfigurable {

    /**
     * Returns null if the URL is to be removed or a normalised representation which can correspond
     * to the input URL.
     *
     * @param sourceUrl the URL of the page where the URL was found. Can be null.
     * @param sourceMetadata the metadata collected for the page
     * @param urlToFilter the URL to be filtered
     * @return null if the url is to be removed or a normalised representation which can correspond
     *     to the input URL
     */
    @Nullable
    public abstract String filter(
            @Nullable URL sourceUrl,
            @Nullable Metadata sourceMetadata,
            @NotNull String urlToFilter);
}
