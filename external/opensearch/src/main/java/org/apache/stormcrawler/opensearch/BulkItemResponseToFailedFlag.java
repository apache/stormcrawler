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

package org.apache.stormcrawler.opensearch;

import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;

/**
 * Wraps a {@link BulkResponseItem} with a pre-computed failure flag. A 409 (conflict) is not
 * considered a failure — it simply indicates a document already existed when using create mode.
 *
 * @param response the original bulk response item
 * @param failed whether this item represents a real failure (excludes 409 conflicts)
 * @param id the document id from the response item
 */
public record BulkItemResponseToFailedFlag(
        @NotNull BulkResponseItem response, boolean failed, @NotNull String id) {

    public BulkItemResponseToFailedFlag {
        Objects.requireNonNull(response, "response");
        Objects.requireNonNull(id, "id");
    }

    /** Constructs with id derived from the response item. */
    public BulkItemResponseToFailedFlag(@NotNull BulkResponseItem response, boolean failed) {
        this(
                response,
                failed,
                Objects.requireNonNull(response.id(), "BulkResponseItem id must not be null"));
    }

    /** Returns the error cause, or {@code null} if the item did not fail. */
    @Nullable
    public ErrorCause getFailedCause() {
        return response.error();
    }

    /** Returns a human-readable failure description, or {@code null} if the item did not fail. */
    @Nullable
    public String getFailure() {
        ErrorCause error = response.error();
        if (error == null) {
            return null;
        }
        return error.reason() != null ? error.reason() : error.type();
    }

    // opensearch-java: status() returns int HTTP code, not RestStatus enum
    /** Returns the HTTP status code of this response item. */
    public int getStatus() {
        return response.status();
    }
}
