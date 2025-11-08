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

import java.util.HashSet;
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
        Metadata md = _filter(parentMetadata, mdToTransfer);

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
        Metadata filteredMetadata = _filter(metadata, mdToTransfer);

        // add the features that are only persisted but
        // not transferred like __redirTo_
        filteredMetadata.putAll(_filter(metadata, mdToPersistOnly));

        return filteredMetadata;
    }

    /**
     * Filter the metadata based on a set of keys. If a key ends with a * then all the keys starting
     * with the prefix will be added.
     */
    private Metadata _filter(Metadata metadata, Set<String> filter) {
        Metadata filteredMetadata = new Metadata();

        for (String key : filter) {
            if (key.endsWith("*")) {
                String prefix = key.substring(0, key.length() - 1);
                for (String k : metadata.keySet(prefix)) {
                    metadata.copy(filteredMetadata, k);
                }
            } else {
                metadata.copy(filteredMetadata, key);
            }
        }

        return filteredMetadata;
    }
}
