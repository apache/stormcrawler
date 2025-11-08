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

package org.apache.stormcrawler.bolt;

import static org.apache.stormcrawler.Constants.StatusStreamName;

import com.google.common.primitives.Bytes;
import crawlercommons.sitemaps.AbstractSiteMap;
import crawlercommons.sitemaps.Namespace;
import crawlercommons.sitemaps.SiteMap;
import crawlercommons.sitemaps.SiteMapIndex;
import crawlercommons.sitemaps.SiteMapParser;
import crawlercommons.sitemaps.SiteMapURL;
import crawlercommons.sitemaps.SiteMapURL.ChangeFrequency;
import crawlercommons.sitemaps.UnknownFormatException;
import crawlercommons.sitemaps.extension.Extension;
import crawlercommons.sitemaps.extension.ExtensionMetadata;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.Outlink;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseFilters;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.stormcrawler.persistence.DefaultScheduler;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.LoggerFactory;

/**
 * Extracts URLs from a sitemap file. The parsing is triggered by sniffing the content and can also
 * be forced by 'isSitemap=true' in the metadata, otherwise the tuple are passed on to the default
 * stream, whereas any URLs extracted from the sitemaps are sent to the 'status' field with a
 * 'DISCOVERED' status.
 */
public class SiteMapParserBolt extends StatusEmitterBolt {

    public static final String isSitemapKey = "isSitemap";
    public static final String foundSitemapKey = "foundSitemap";

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SiteMapParserBolt.class);

    private static final byte[] clue = Namespace.SITEMAP.getBytes(StandardCharsets.UTF_8);

    private SiteMapParser parser;

    private ParseFilter parseFilters;
    private int filterHoursSinceModified = -1;

    private int maxOffsetGuess = 300;

    private ReducedMetric averagedMetrics;

    /** Delay in minutes used for scheduling sub-sitemaps. */
    private int scheduleSitemapsWithDelay = -1;

    private List<Extension> extensionsToParse;

    @Override
    public void execute(Tuple tuple) {
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");
        byte[] content = tuple.getBinaryByField("content");
        String url = tuple.getStringByField("url");

        String ct = metadata.getFirstValue(HttpHeaders.CONTENT_TYPE);

        LOG.debug("Processing {}", url);

        boolean looksLikeSitemap = sniff(content);
        // can force the mimetype as we know it is XML
        if (looksLikeSitemap) {
            ct = "application/xml";
        }

        String isSitemap = metadata.getFirstValue(isSitemapKey);

        boolean treatAsSitemap = Boolean.parseBoolean(isSitemap);

        // doesn't have the key and want to rely on the clue
        if (isSitemap == null && looksLikeSitemap) {
            LOG.info("{} detected as sitemap based on content", url);
            treatAsSitemap = true;
        }

        // decided that it is not a sitemap file
        if (!treatAsSitemap) {
            LOG.debug("Not a sitemap {}", url);
            // just pass it on
            metadata.setValue(isSitemapKey, "false");
            this.collector.emit(tuple, tuple.getValues());
            this.collector.ack(tuple);
            return;
        }

        // mark the current doc as a sitemap
        // as it won't have the k/v if it is a redirected sitemap
        metadata.setValue(isSitemapKey, "true");

        List<Outlink> outlinks;
        try {
            outlinks = parseSiteMap(url, content, ct, metadata);
        } catch (Exception e) {
            // exception while parsing the sitemap
            String errorMessage = "Exception while parsing " + url + ": " + e;
            LOG.error(errorMessage);
            // send to status stream in case another component wants to update
            // its status
            metadata.setValue(Constants.STATUS_ERROR_SOURCE, "sitemap parsing");
            metadata.setValue(Constants.STATUS_ERROR_MESSAGE, errorMessage);
            collector.emit(
                    Constants.StatusStreamName, tuple, new Values(url, metadata, Status.ERROR));
            collector.ack(tuple);
            return;
        }

        // apply the parse filters if any to the current document
        ParseResult parse = new ParseResult(outlinks);
        parse.set(url, metadata);

        // apply the parse filters if any
        try {
            parseFilters.filter(url, content, null, parse);
        } catch (RuntimeException e) {
            String errorMessage = "Exception while running parse filters on " + url + ": " + e;
            LOG.error(errorMessage);
            metadata.setValue(Constants.STATUS_ERROR_SOURCE, "content filtering");
            metadata.setValue(Constants.STATUS_ERROR_MESSAGE, errorMessage);
            collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.ERROR));
            collector.ack(tuple);
            return;
        }

        // send to status stream
        for (Outlink ol : parse.getOutlinks()) {
            Values v = new Values(ol.getTargetURL(), ol.getMetadata(), Status.DISCOVERED);
            collector.emit(Constants.StatusStreamName, tuple, v);
        }

        // marking the main URL as successfully fetched
        // regardless of whether we got a parse exception or not
        collector.emit(
                Constants.StatusStreamName, tuple, new Values(url, metadata, Status.FETCHED));
        collector.ack(tuple);
    }

    private List<Outlink> parseSiteMap(
            String url, byte[] content, String contentType, Metadata parentMetadata)
            throws UnknownFormatException, IOException {

        URL url1 = new URL(url);
        long start = System.currentTimeMillis();
        AbstractSiteMap siteMap;
        // let the parser guess what the mimetype is
        if (StringUtils.isBlank(contentType) || contentType.contains("octet-stream")) {
            siteMap = parser.parseSiteMap(content, url1);
        } else {
            siteMap = parser.parseSiteMap(contentType, content, url1);
        }
        long end = System.currentTimeMillis();
        averagedMetrics.update(end - start);

        List<Outlink> links = new ArrayList<>();

        if (siteMap.isIndex()) {
            SiteMapIndex smi = (SiteMapIndex) siteMap;
            Collection<AbstractSiteMap> subsitemaps = smi.getSitemaps();

            Calendar rightNow = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
            rightNow.add(Calendar.HOUR, -filterHoursSinceModified);

            int delay = 0;

            // keep the subsitemaps as outlinks
            // they will be fetched and parsed in the following steps
            for (AbstractSiteMap asm : subsitemaps) {
                String target = asm.getUrl().toExternalForm();

                Date lastModified = asm.getLastModified();
                String lastModifiedValue = "";
                if (lastModified != null) {
                    // filter based on the published date
                    if (filterHoursSinceModified != -1) {
                        if (lastModified.before(rightNow.getTime())) {
                            LOG.info(
                                    "{} has a modified date {} which is more than {} hours old",
                                    target,
                                    lastModified,
                                    filterHoursSinceModified);
                            continue;
                        }
                    }
                    lastModifiedValue = lastModified.toString();
                }

                Outlink ol =
                        filterOutlink(
                                url1,
                                target,
                                parentMetadata,
                                isSitemapKey,
                                "true",
                                "sitemap.lastModified",
                                lastModifiedValue);
                if (ol == null) {
                    continue;
                }

                // add a delay
                if (this.scheduleSitemapsWithDelay > 0) {
                    if (delay > 0) {
                        ol.getMetadata()
                                .setValue(DefaultScheduler.DELAY_METADATA, Integer.toString(delay));
                    }
                    delay += this.scheduleSitemapsWithDelay;
                }

                links.add(ol);
                LOG.debug("{} : [sitemap] {}", url, target);
            }
        } else {
            // sitemap files
            SiteMap sm = (SiteMap) siteMap;
            // TODO: see what we can do with the LastModified info
            Collection<SiteMapURL> sitemapUrls = sm.getSiteMapUrls();
            for (SiteMapURL smurl : sitemapUrls) {
                // TODO: handle priority in metadata
                double priority = smurl.getPriority();
                // TODO: convert the frequency into a numerical value and handle
                // it in metadata
                ChangeFrequency freq = smurl.getChangeFrequency();

                String target = smurl.getUrl().toExternalForm();
                String lastModifiedValue = "";
                Date lastModified = smurl.getLastModified();
                if (lastModified != null) {
                    // filter based on the published date
                    if (filterHoursSinceModified != -1) {
                        Calendar rightNow =
                                Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
                        rightNow.add(Calendar.HOUR, -filterHoursSinceModified);
                        if (lastModified.before(rightNow.getTime())) {
                            LOG.info(
                                    "{} has a modified date {} which is more than {} hours old",
                                    target,
                                    lastModified.toString(),
                                    filterHoursSinceModified);
                            continue;
                        }
                    }
                    lastModifiedValue = lastModified.toString();
                }

                Outlink ol =
                        filterOutlink(
                                url1,
                                target,
                                parentMetadata,
                                isSitemapKey,
                                "false",
                                "sitemap.lastModified",
                                lastModifiedValue);

                if (ol == null) {
                    continue;
                }
                parseExtensionAttributes(smurl, ol.getMetadata());
                links.add(ol);
                LOG.debug("{} : [sitemap] {}", url, target);
            }
        }

        return links;
    }

    public void parseExtensionAttributes(SiteMapURL url, Metadata metadata) {

        for (Extension extension : extensionsToParse) {
            ExtensionMetadata[] extensionMetadata = url.getAttributesForExtension(extension);

            if (extensionMetadata != null) {

                for (ExtensionMetadata extensionMetadatum : extensionMetadata) {

                    for (Map.Entry<String, String[]> entry :
                            extensionMetadatum.asMap().entrySet()) {

                        if (entry.getValue() != null) {
                            metadata.addValues(
                                    extension.name() + "." + entry.getKey(), entry.getValue());
                        }
                    }
                }
            }
        }
    }

    @Override
    public void prepare(
            Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        parser = new SiteMapParser(false);
        filterHoursSinceModified =
                ConfUtils.getInt(stormConf, "sitemap.filter.hours.since.modified", -1);
        parseFilters = ParseFilters.fromConf(stormConf);
        maxOffsetGuess = ConfUtils.getInt(stormConf, "sitemap.offset.guess", 300);
        averagedMetrics =
                context.registerMetric(
                        "sitemap_average_processing_time",
                        new ReducedMetric(new MeanReducer()),
                        30);
        scheduleSitemapsWithDelay =
                ConfUtils.getInt(stormConf, "sitemap.schedule.delay", scheduleSitemapsWithDelay);
        List<String> extensionsStrings =
                ConfUtils.loadListFromConf("sitemap.extensions", stormConf);
        extensionsToParse = new ArrayList<>(extensionsStrings.size());

        for (String type : extensionsStrings) {
            Extension extension = Extension.valueOf(type);
            parser.enableExtension(extension);
            extensionsToParse.add(extension);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declare(new Fields("url", "content", "metadata"));
    }

    /**
     * Examines the first bytes of the content for a clue of whether this document is a sitemap,
     * based on namespaces. Works for XML and non-compressed documents only.
     */
    private boolean sniff(byte[] content) {
        byte[] beginning = content;
        if (content.length > maxOffsetGuess && maxOffsetGuess > 0) {
            beginning = Arrays.copyOfRange(content, 0, maxOffsetGuess);
        }
        int position = Bytes.indexOf(beginning, clue);
        return position != -1;
    }

    @Override
    public void cleanup() {
        if (parseFilters != null) {
            parseFilters.cleanup();
        }
    }
}
