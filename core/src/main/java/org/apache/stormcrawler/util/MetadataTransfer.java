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

package org.apache.stormcrawler.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;

/**
 * Implements the logic of how the metadata should be passed to the outlinks, what should be stored
 * back in the persistence layer etc...
 */
public class MetadataTransfer {

    /**
     * Class to use for transferring metadata to outlinks. Must extend the class MetadataTransfer.
     */
    public static final String metadataTransferClassParamName = "metadata.transfer.class";

    /**
     * Parameter name indicating which metadata to transfer to the outlinks and persist for a given
     * document. Value is either a vector or a single valued String.
     */
    public static final String metadataTransferParamName = "metadata.transfer";

    /**
     * Parameter name indicating which metadata to persist for a given document but <b>not</b>
     * transfer to outlinks. Value is either a vector or a single valued String.
     */
    public static final String metadataPersistParamName = "metadata.persist";

    /**
     * Parameter name indicating whether to track the url path or not. Boolean value, true by
     * default.
     */
    public static final String trackPathParamName = "metadata.track.path";

    /**
     * Parameter name indicating whether to track the depth from seed. Boolean value, true by
     * default.
     */
    public static final String trackDepthParamName = "metadata.track.depth";

    /** Metadata key name for tracking the source URLs. */
    public static final String urlPathKeyName = "url.path";

    /** Metadata key name for tracking the depth. */
    public static final String depthKeyName = "depth";

    /** Metadata key name for tracking a non-default max depth. */
    public static final String maxDepthKeyName = "max.depth";

    protected final Set<String> mdToTransfer = new HashSet<>();

    protected final Set<String> mdToPersistOnly = new HashSet<>();

    protected boolean trackPath = true;

    protected boolean trackDepth = true;

    public static MetadataTransfer getInstance(Map<String, Object> conf) {
        String className = ConfUtils.getString(conf, metadataTransferClassParamName);

        MetadataTransfer transferInstance;

        // no custom class specified
        if (StringUtils.isBlank(className)) {
            transferInstance = new MetadataTransfer();
        } else {
            transferInstance =
                    InitialisationUtil.initializeFromQualifiedName(
                            className, MetadataTransfer.class);
        }

        transferInstance.configure(conf);

        return transferInstance;
    }

    protected void configure(Map<String, Object> conf) {

        trackPath = ConfUtils.getBoolean(conf, trackPathParamName, true);

        trackDepth = ConfUtils.getBoolean(conf, trackDepthParamName, true);

        // keep the path but don't add anything to it
        if (trackPath) {
            mdToTransfer.add(urlPathKeyName);
        }

        // keep the depth but don't add anything to it
        if (trackDepth) {
            mdToTransfer.add(depthKeyName);
            mdToTransfer.add(maxDepthKeyName);
        }

        mdToTransfer.addAll(ConfUtils.loadListFromConf(metadataTransferParamName, conf));
        mdToPersistOnly.addAll(ConfUtils.loadListFromConf(metadataPersistParamName, conf));
        // always add the fetch error count
        mdToPersistOnly.add(Constants.fetchErrorCountParamName);
    }

    /**
     * Determine which metadata should be transferred to an outlink. Adds additional metadata like
     * the URL path.
     */
    public Metadata getMetaForOutlink(String targetUrl, String sourceUrl, Metadata parentMetadata) {
        Metadata md = filter(parentMetadata, transferFilter());

        // keep the path?
        if (trackPath) {
            md.addValue(urlPathKeyName, sourceUrl);
        }

        // track depth
        if (trackDepth) {
            String existingDepth = md.getFirstValue(depthKeyName);
            int depth;
            try {
                depth = Integer.parseInt(existingDepth);
            } catch (Exception e) {
                depth = 0;
            }
            md.setValue(depthKeyName, Integer.toString(++depth));
        }

        return md;
    }

    /**
     * Determine which metadata should be persisted for a given document including those which are
     * not necessarily transferred to the outlinks.
     */
    public Metadata filter(Metadata metadata) {
        Metadata filteredMetadata = filter(metadata, transferFilter());

        // add the features that are only persisted but
        // not transferred like __redirTo_
        filteredMetadata.putAll(filter(metadata, persistOnlyFilter()));

        return filteredMetadata;
    }

    /**
     * Filter the metadata based on a set of keys. If a key ends with a * then all the keys starting
     * with the prefix will be added.
     */
    private static Metadata filter(Metadata metadata, CompiledFilter compiled) {
        final Map<String, String[]> source = metadata.asMap();
        final Map<String, String[]> target = new HashMap<>();

        // exact keys: direct lookups
        for (String key : compiled.exactKeys) {
            final String[] values = source.get(key);
            if (values != null && values.length > 0) {
                target.put(key, values);
            }
        }

        // wildcards: a single pass over the metadata for all the prefixes,
        // without allocating an intermediate key set per prefix
        if (compiled.prefixes.length > 0) {
            for (Map.Entry<String, String[]> entry : source.entrySet()) {
                final String key = entry.getKey();
                final String[] values = entry.getValue();
                if (values == null || values.length == 0 || target.containsKey(key)) {
                    continue;
                }
                for (String prefix : compiled.prefixes) {
                    if (key.startsWith(prefix)) {
                        target.put(key, values);
                        break;
                    }
                }
            }
        }

        return new Metadata(target);
    }

    /**
     * Pre-computed, normalised form of a set of keys to transfer: exact keys and wildcard prefixes.
     * Keeps a snapshot of the keys it was built from so that a stale instance can be detected.
     */
    private static final class CompiledFilter {
        private final Set<String> snapshot;
        private final Set<String> exactKeys;
        private final String[] prefixes;

        private CompiledFilter(Set<String> filter) {
            this.snapshot = new HashSet<>(filter);
            final Set<String> exact = new HashSet<>();
            final List<String> prefixList = new ArrayList<>();
            for (String key : snapshot) {
                final String normalised = key.toLowerCase(Locale.ROOT);
                if (normalised.endsWith("*")) {
                    prefixList.add(normalised.substring(0, normalised.length() - 1));
                } else {
                    exact.add(normalised);
                }
            }
            this.exactKeys = exact;
            this.prefixes = prefixList.toArray(new String[0]);
        }

        /** True if this instance was built from exactly the given keys. */
        private boolean isFor(Set<String> filter) {
            return snapshot.equals(filter);
        }
    }

    // Built lazily on first use and rebuilt whenever the underlying set no longer matches the
    // snapshot, so subclasses may keep editing mdToTransfer / mdToPersistOnly at any time. The
    // equality check is a handful of hash lookups with no allocation; the compile itself only
    // runs when the keys actually changed.
    private CompiledFilter compiledTransfer;
    private CompiledFilter compiledPersistOnly;

    private CompiledFilter transferFilter() {
        CompiledFilter compiled = compiledTransfer;
        if (compiled == null || !compiled.isFor(mdToTransfer)) {
            compiled = new CompiledFilter(mdToTransfer);
            compiledTransfer = compiled;
        }
        return compiled;
    }

    private CompiledFilter persistOnlyFilter() {
        CompiledFilter compiled = compiledPersistOnly;
        if (compiled == null || !compiled.isFor(mdToPersistOnly)) {
            compiled = new CompiledFilter(mdToPersistOnly);
            compiledPersistOnly = compiled;
        }
        return compiled;
    }
}
