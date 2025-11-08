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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.DocumentFragmentBuilder;
import org.apache.stormcrawler.parse.JSoupFilter;
import org.apache.stormcrawler.parse.JSoupFilters;
import org.apache.stormcrawler.parse.JSoupTextExtractor;
import org.apache.stormcrawler.parse.Outlink;
import org.apache.stormcrawler.parse.ParseData;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseFilters;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.stormcrawler.parse.TextExtractor;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.util.CharsetIdentification;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.RefreshTag;
import org.apache.stormcrawler.util.RobotsTags;
import org.apache.stormcrawler.util.URLUtil;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

/**
 * Parser for HTML documents only which uses ICU4J to detect the charset encoding. Kindly donated to
 * stormcrawler by shopstyle.com.
 */
public class JSoupParserBolt extends StatusEmitterBolt {

    /** Metadata key name for tracking the anchors. */
    public static final String ANCHORS_KEY_NAME = "anchors";

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(JSoupParserBolt.class);

    private MultiCountMetric eventCounter;

    private ParseFilter parseFilters = null;

    private JSoupFilter jsoupFilters = null;

    private final Detector detector = TikaConfig.getDefaultConfig().getDetector();

    private boolean detectMimeType = true;

    private boolean trackAnchors = true;

    private boolean emitOutlinks = true;

    private int maxOutlinksPerPage = -1;

    private boolean robotsNoFollowStrict = true;

    /**
     * If a Tuple is not HTML whether to send it to the status stream as an error or pass it on the
     * default stream.
     */
    private boolean treatNonHtmlAsError = true;

    /**
     * Length of content to use for detecting the charset. Set to -1 to use the full content (will
     * make the parser slow), 0 to deactivate the detection altogether, or any other value (at least
     * a few hundred bytes).
     */
    private int maxLengthCharsetDetection = -1;

    private TextExtractor textExtractor;

    private String protocolMetadataPrefix;

    private boolean robotsHeaderSkip;

    private boolean robotsMetaSkip;

    private boolean fastCharsetDetection;

    private boolean ignoreMetaRedirections;

    @Override
    public void prepare(
            Map<String, Object> conf, TopologyContext context, OutputCollector collector) {

        super.prepare(conf, context, collector);

        eventCounter =
                context.registerMetric(this.getClass().getSimpleName(), new MultiCountMetric(), 10);

        parseFilters = ParseFilters.fromConf(conf);

        jsoupFilters = JSoupFilters.fromConf(conf);

        emitOutlinks = ConfUtils.getBoolean(conf, "parser.emitOutlinks", true);

        trackAnchors = ConfUtils.getBoolean(conf, "track.anchors", true);

        robotsNoFollowStrict = ConfUtils.getBoolean(conf, RobotsTags.ROBOTS_NO_FOLLOW_STRICT, true);

        treatNonHtmlAsError = ConfUtils.getBoolean(conf, "jsoup.treat.non.html.as.error", true);

        detectMimeType = ConfUtils.getBoolean(conf, "detect.mimetype", true);

        maxLengthCharsetDetection = ConfUtils.getInt(conf, "detect.charset.maxlength", -1);

        fastCharsetDetection = ConfUtils.getBoolean(conf, "detect.charset.fast", false);

        maxOutlinksPerPage = ConfUtils.getInt(conf, "parser.emitOutlinks.max.per.page", -1);

        protocolMetadataPrefix =
                ConfUtils.getString(conf, ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "");

        robotsHeaderSkip = ConfUtils.getBoolean(conf, "http.robots.headers.skip", false);

        robotsMetaSkip = ConfUtils.getBoolean(conf, "http.robots.meta.skip", false);

        ignoreMetaRedirections =
                ConfUtils.getBoolean(conf, "jsoup.ignore.meta.redirections", false);

        final String clazz =
                ConfUtils.getString(
                        conf, "textextractor.class", JSoupTextExtractor.class.getName());
        try {
            textExtractor =
                    (TextExtractor)
                            Class.forName(clazz)
                                    .getDeclaredConstructor(Map.class)
                                    .newInstance(conf);
        } catch (ClassNotFoundException e) {
            LOG.warn("Could not load configured textextractor.class '{}'.", clazz, e);
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            LOG.warn(
                    "Configured textextractor.class '{}' "
                            + "does not provide a Map argument constructor.",
                    clazz,
                    e);
            throw new RuntimeException(e);
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
            LOG.warn("Cannot instantiazr textextractor.class '{}'.", clazz, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple tuple) {

        final byte[] content = tuple.getBinaryByField("content");
        final String url = tuple.getStringByField("url");
        final Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        LOG.info("Parsing : starting {}", url);

        // check that its content type is HTML
        // look at value found in HTTP headers
        boolean contentTypeIsOk = false;

        String mimeType =
                metadata.getFirstValue(HttpHeaders.CONTENT_TYPE, this.protocolMetadataPrefix);

        if (detectMimeType) {
            try {
                mimeType = guessMimeType(url, mimeType, content);
            } catch (Exception e) {
                String errorMessage = "Exception while guessing mimetype on " + url + ": " + e;
                handleException(url, e, metadata, tuple, "mimetype guessing", errorMessage);
                return;
            }
            // store identified type in md
            metadata.setValue("parse.Content-Type", mimeType);
        }

        if (StringUtils.isNotBlank(mimeType)) {
            if (mimeType.toLowerCase(Locale.ROOT).contains("html")) {
                contentTypeIsOk = true;
            }
        }
        // go ahead even if no mimetype is available
        else {
            contentTypeIsOk = true;
        }

        if (!contentTypeIsOk) {
            if (this.treatNonHtmlAsError) {
                String errorMessage = "Exception content-type " + mimeType + " for " + url;
                RuntimeException e = new RuntimeException(errorMessage);
                handleException(url, e, metadata, tuple, "content-type checking", errorMessage);
            } else {
                LOG.info("Unsupported mimetype {} - passing on : {}", mimeType, url);
                collector.emit(tuple, new Values(url, content, metadata, ""));
                collector.ack(tuple);
            }
            return;
        }

        long start = System.currentTimeMillis();

        String charset;

        if (fastCharsetDetection) {
            charset =
                    CharsetIdentification.getCharsetFast(
                            metadata, content, maxLengthCharsetDetection);
        } else {
            charset =
                    CharsetIdentification.getCharset(metadata, content, maxLengthCharsetDetection);
        }

        LOG.debug(
                "Charset identified as {} in {} msec",
                charset,
                (System.currentTimeMillis() - start));

        RobotsTags robotsTags = new RobotsTags();

        // get the robots tags from the fetch metadata
        if (!robotsHeaderSkip) {
            robotsTags = new RobotsTags(metadata, this.protocolMetadataPrefix);
        }

        Map<String, List<String>> slinks;
        String text;
        final org.jsoup.nodes.Document jsoupDoc;

        try {
            String html = Charset.forName(charset).decode(ByteBuffer.wrap(content)).toString();

            jsoupDoc = Parser.htmlParser().parseInput(html, url);

            if (!robotsMetaSkip) {
                // extracts the robots directives from the meta tags
                Element robotelement = jsoupDoc.selectFirst("meta[name~=(?i)robots][content]");
                if (robotelement != null) {
                    robotsTags.extractMetaTags(robotelement.attr("content"));
                }
            }

            // store a normalised representation in metadata
            // so that the indexer is aware of it
            robotsTags.normaliseToMetadata(metadata);

            // do not extract the links if no follow has been set
            // and we are in strict mode
            if (robotsTags.isNoFollow() && robotsNoFollowStrict) {
                slinks = new HashMap<>(0);
            } else {
                final Elements links = jsoupDoc.select("a[href]");
                slinks = new HashMap<>(links.size());
                final URL baseUrl = new URL(url);
                for (Element link : links) {
                    // nofollow
                    String[] relkeywords = link.attr("rel").split(" ");
                    boolean noFollow =
                            Stream.of(relkeywords).anyMatch(x -> x.equalsIgnoreCase("nofollow"));

                    // remove altogether
                    if (noFollow && robotsNoFollowStrict) {
                        continue;
                    }

                    // link not specifically marked as no follow
                    // but whole page is
                    if (!noFollow && robotsTags.isNoFollow()) {
                        noFollow = true;
                    }

                    String targetUrl = null;

                    try {
                        // abs:href tells jsoup to return fully qualified domains
                        // for relative urls
                        // but it is very slow as it builds intermediate URL objects
                        // and normalises the URL of the document every time
                        targetUrl = URLUtil.resolveUrl(baseUrl, link.attr("href")).toExternalForm();
                    } catch (MalformedURLException e) {
                        LOG.debug(
                                "Cannot resolve URL with baseURL : {} and href : {}",
                                baseUrl,
                                link.attr("href"),
                                e);
                    }

                    if (StringUtils.isBlank(targetUrl)) {
                        continue;
                    }

                    final List<String> anchors =
                            slinks.computeIfAbsent(targetUrl, a -> new LinkedList<>());

                    // any existing anchors for the same target?
                    final String anchor = link.text();
                    // track the anchors only if no follow is false
                    if (!noFollow && StringUtils.isNotBlank(anchor)) {
                        anchors.add(anchor);
                    }
                }
            }

            Element body = jsoupDoc.body();
            text = textExtractor.text(body);

        } catch (Throwable e) {
            String errorMessage = "Exception while parsing " + url + ": " + e;
            handleException(url, e, metadata, tuple, "content parsing", errorMessage);
            return;
        }

        // store identified charset in md
        metadata.setValue("parse.Content-Encoding", charset);

        // track that is has been successfully handled
        metadata.setValue("parsed.by", this.getClass().getName());

        long duration = System.currentTimeMillis() - start;

        LOG.info("Parsed {} in {} msec", url, duration);

        // redirection?
        if (!ignoreMetaRedirections) {
            try {
                final String redirection = RefreshTag.extractRefreshURL(jsoupDoc);
                if (StringUtils.isNotBlank(redirection)) {
                    // stores the URL it redirects to
                    // used for debugging mainly - do not resolve the target
                    // URL
                    LOG.info("Found redir in {} to {}", url, redirection);
                    metadata.setValue("_redirTo", redirection);

                    // https://github.com/apache/stormcrawler/issues/954
                    if (allowRedirs() && StringUtils.isNotBlank(redirection)) {
                        emitOutlink(tuple, new URL(url), redirection, metadata);
                    }

                    // Mark URL as redirected
                    collector.emit(
                            org.apache.stormcrawler.Constants.StatusStreamName,
                            tuple,
                            new Values(url, metadata, Status.REDIRECTION));
                    collector.ack(tuple);
                    eventCounter.scope("tuple_success").incr();
                    return;
                }
            } catch (MalformedURLException e) {
                LOG.error("MalformedURLException on {}", url);
            }
        }

        List<Outlink> outlinks = toOutlinks(url, metadata, slinks);

        ParseResult parse = new ParseResult(outlinks);

        // parse data of the parent URL
        ParseData parseData = parse.get(url);
        parseData.setMetadata(metadata);
        parseData.setText(text);
        parseData.setContent(content);

        // apply the JSoup filters if any
        try {
            jsoupFilters.filter(url, content, jsoupDoc, parse);
        } catch (RuntimeException e) {
            String errorMessage = "Exception while running jsoup filters on " + url + ": " + e;
            handleException(url, e, metadata, tuple, "jsoup filtering", errorMessage);
            return;
        }

        // apply the parse filters if any
        try {
            DocumentFragment fragment = null;
            // lazy building of fragment
            if (parseFilters.needsDOM()) {
                fragment = DocumentFragmentBuilder.fromJsoup(jsoupDoc);
            }
            parseFilters.filter(url, content, fragment, parse);
        } catch (RuntimeException e) {
            String errorMessage = "Exception while running parse filters on " + url + ": " + e;
            handleException(url, e, metadata, tuple, "content filtering", errorMessage);
            return;
        }

        if (emitOutlinks) {
            for (Outlink outlink : parse.getOutlinks()) {
                collector.emit(
                        StatusStreamName,
                        tuple,
                        new Values(
                                outlink.getTargetURL(), outlink.getMetadata(), Status.DISCOVERED));
            }
        }

        // emit each document/subdocument in the ParseResult object
        // there should be at least one ParseData item for the "parent" URL

        for (Map.Entry<String, ParseData> doc : parse) {
            ParseData parseDoc = doc.getValue();
            collector.emit(
                    tuple,
                    new Values(
                            doc.getKey(),
                            parseDoc.getContent(),
                            parseDoc.getMetadata(),
                            parseDoc.getText()));
        }

        LOG.info("Total for {} - {} msec", url, System.currentTimeMillis() - start);

        collector.ack(tuple);
        eventCounter.scope("tuple_success").incr();
    }

    private void handleException(
            String url,
            Throwable e,
            Metadata metadata,
            Tuple tuple,
            String errorSource,
            String errorMessage) {
        LOG.error(errorMessage);
        // send to status stream in case another component wants to update
        // its status
        metadata.setValue(Constants.STATUS_ERROR_SOURCE, errorSource);
        metadata.setValue(Constants.STATUS_ERROR_MESSAGE, errorMessage);
        collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.ERROR));
        collector.ack(tuple);
        // Increment metric that is context specific
        String s = "error_" + errorSource.replaceAll(" ", "_") + "_";
        eventCounter.scope(s + e.getClass().getSimpleName()).incrBy(1);
        // Increment general metric
        eventCounter.scope("parse exception").incrBy(1);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        // output of this module is the list of fields to index
        // with at least the URL, text content
        declarer.declare(new Fields("url", "content", "metadata", "text"));
    }

    public String guessMimeType(String url, String httpContentType, byte[] content) {

        org.apache.tika.metadata.Metadata metadata = new org.apache.tika.metadata.Metadata();

        if (StringUtils.isNotBlank(httpContentType)) {
            // pass content type from server as a clue
            metadata.set(org.apache.tika.metadata.Metadata.CONTENT_TYPE, httpContentType);
        }

        // use full URL as a clue
        metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, url);

        metadata.set(
                org.apache.tika.metadata.Metadata.CONTENT_LENGTH, Integer.toString(content.length));

        try (InputStream stream = new ByteArrayInputStream(content)) {
            MediaType mt = detector.detect(stream, metadata);
            return mt.toString();
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected IOException", e);
        }
    }

    protected List<Outlink> toOutlinks(
            String url, Metadata metadata, Map<String, List<String>> slinks) {

        if (slinks.size() == 0) {
            return new LinkedList<>();
        }

        URL sourceUrl;
        try {
            sourceUrl = new URL(url);
        } catch (MalformedURLException e) {
            // we would have known by now as previous components check whether
            // the URL is valid
            LOG.error("MalformedURLException on {}", url);
            eventCounter.scope("error_invalid_source_url").incrBy(1);
            return new LinkedList<>();
        }

        final Map<String, Outlink> outlinks = new HashMap<>();

        for (Map.Entry<String, List<String>> linkEntry : slinks.entrySet()) {

            // got enough
            if (maxOutlinksPerPage >= 0 && outlinks.size() >= maxOutlinksPerPage) {
                LOG.info(
                        "Found {} unique links for {} trimming to {}",
                        slinks.size(),
                        url,
                        maxOutlinksPerPage);
                break;
            }

            String targetUrl = linkEntry.getKey();

            Outlink ol = filterOutlink(sourceUrl, targetUrl, metadata);
            if (ol == null) {
                eventCounter.scope("outlink_filtered").incr();
                continue;
            }

            // the same link could already be there post-normalisation
            Outlink old = outlinks.get(ol.getTargetURL());
            if (old != null) {
                ol = old;
            }

            List<String> anchors = linkEntry.getValue();
            if (trackAnchors && anchors.size() > 0) {
                ol.getMetadata().addValues(ANCHORS_KEY_NAME, anchors);
                // sets the first anchor
                ol.setAnchor(anchors.get(0));
            }

            if (old == null) {
                outlinks.put(ol.getTargetURL(), ol);
                eventCounter.scope("outlink_kept").incr();
            }
        }

        return new LinkedList<>(outlinks.values());
    }

    @Override
    public void cleanup() {
        if (parseFilters != null) {
            parseFilters.cleanup();
        }
    }
}
