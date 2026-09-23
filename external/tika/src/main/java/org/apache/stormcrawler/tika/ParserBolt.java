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

package org.apache.stormcrawler.tika;

import static org.apache.stormcrawler.Constants.StatusStreamName;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.http.HttpHeaders;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Constants;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.filtering.URLFilters;
import org.apache.stormcrawler.metrics.CrawlerMetrics;
import org.apache.stormcrawler.metrics.ScopedCounter;
import org.apache.stormcrawler.parse.Outlink;
import org.apache.stormcrawler.parse.ParseData;
import org.apache.stormcrawler.parse.ParseFilter;
import org.apache.stormcrawler.parse.ParseFilters;
import org.apache.stormcrawler.parse.ParseResult;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.InitialisationUtil;
import org.apache.stormcrawler.util.MetadataTransfer;
import org.apache.stormcrawler.util.URLUtil;
import org.apache.tika.Tika;
import org.apache.tika.config.loader.TikaLoader;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.WriteLimitReachedException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.EmptyParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.html.IdentityHtmlMapper;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.apache.tika.sax.Link;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/** Uses Tika to parse the output of a fetch and extract text + metadata. */
public class ParserBolt extends BaseRichBolt {

    /**
     * Configuration key for the maximum number of characters of text extracted from a document, a
     * negative value for no limit.
     */
    public static final String TEXT_MAX_LENGTH_PARAM = "parser.tika.text.maxlength";

    /**
     * Metadata key set to "true" when the extracted text has been cut at {@link
     * #TEXT_MAX_LENGTH_PARAM}.
     */
    public static final String TEXT_TRIMMED_KEY = "parse.text.trimmed";

    /**
     * Configuration key for the maximum time in milliseconds a document may take to parse, a value
     * of 0 or less for no limit.
     */
    public static final String PARSE_TIMEOUT_PARAM = "parser.tika.timeout";

    private static final AtomicInteger PARSE_THREAD_COUNT = new AtomicInteger();

    private Tika tika;

    /** ParseContext configured from the "parse-context" section of the Tika configuration. */
    private ParseContext configuredParseContext = new ParseContext();

    private URLFilters urlFilters = null;
    private ParseFilter parseFilters = null;

    private OutputCollector collector;

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ParserBolt.class);

    private ScopedCounter eventCounter;

    private boolean upperCaseElementNames = true;
    private Class<? extends HtmlMapper> htmlMapperClass = IdentityHtmlMapper.class;

    private boolean extractEmbedded = false;

    private MetadataTransfer metadataTransfer;
    private boolean emitOutlinks = true;

    /** regular expressions to apply to the mime-type * */
    private List<Pattern> mimeTypeWhiteList = new LinkedList<>();

    private String protocolMDprefix;

    private int textMaxLength = -1;

    private long parseTimeout = -1;

    /** runs the parses when a timeout is set, replaced after each timeout */
    private ExecutorService parseExecutor;

    @Override
    public void prepare(
            @NotNull Map<String, Object> conf,
            @NotNull TopologyContext context,
            @NotNull OutputCollector collector) {

        emitOutlinks = ConfUtils.getBoolean(conf, "parser.emitOutlinks", true);

        urlFilters = URLFilters.fromConf(conf);

        parseFilters = ParseFilters.fromConf(conf);

        upperCaseElementNames = ConfUtils.getBoolean(conf, "parser.uppercase.element.names", true);

        extractEmbedded = ConfUtils.getBoolean(conf, "parser.extract.embedded", false);

        String htmlmapperClassName =
                ConfUtils.getString(
                        conf,
                        "parser.htmlmapper.classname",
                        "org.apache.tika.parser.html.IdentityHtmlMapper");

        try {
            htmlMapperClass = InitialisationUtil.getClassFor(htmlmapperClassName, HtmlMapper.class);
        } catch (RuntimeException e) {
            LOG.error("Can't load class {}", htmlmapperClassName);
            throw e;
        }

        final List<String> mimeTypeWhiteListStrings =
                ConfUtils.loadListFromConf("parser.mimetype.whitelist", conf);
        for (String mt : mimeTypeWhiteListStrings) {
            try {
                this.mimeTypeWhiteList.add(Pattern.compile(mt));
            } catch (RuntimeException e) {
                LOG.warn("Failed to compile whitelist regex: {}", mt);
            }
        }

        protocolMDprefix = ConfUtils.getString(conf, ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "");

        // Tika only treats exactly -1 as unlimited and fails on other negative values
        int maxLength = ConfUtils.getInt(conf, TEXT_MAX_LENGTH_PARAM, -1);
        textMaxLength = maxLength < 0 ? -1 : maxLength;

        parseTimeout = ConfUtils.getLong(conf, PARSE_TIMEOUT_PARAM, -1);
        if (parseTimeout > 0) {
            parseExecutor = newParseExecutor();
        }

        tika = instantiateTika(conf);

        this.collector = collector;

        this.eventCounter =
                CrawlerMetrics.registerCounter(context, conf, this.getClass().getSimpleName(), 10);

        this.metadataTransfer = MetadataTransfer.getInstance(conf);
    }

    @Override
    public void execute(Tuple tuple) {
        eventCounter.scope("tuple_in").incrBy(1);

        byte[] content = tuple.getBinaryByField("content");

        String url = tuple.getStringByField("url");
        Metadata metadata = (Metadata) tuple.getValueByField("metadata");

        // check that the mimetype is in the whitelist
        if (!mimeTypeWhiteList.isEmpty()) {
            boolean mt_match = false;
            // parse.Content-Type is assumed byte-detected (JSoupParserBolt uses Tika detection,
            // not the raw server header). A custom upstream writing a header-copied value bypasses
            // this check — that is a caller responsibility.
            String mimeType = metadata.getFirstValue("parse.Content-Type");
            if (mimeType == null) {
                // parse.Content-Type is absent: detect from content bytes so that
                // the whitelist is evaluated against the same type Tika will use
                // to select a parser, not the server-declared HTTP header which is
                // untrusted and may differ from what the bytes actually are.
                String httpCTHint =
                        metadata.getFirstValue(HttpHeaders.CONTENT_TYPE, this.protocolMDprefix);
                org.apache.tika.metadata.Metadata detectionMd =
                        new org.apache.tika.metadata.Metadata();
                if (StringUtils.isNotBlank(httpCTHint)) {
                    // the hint narrows the magic result: when bytes are unrecognised Tika
                    // returns application/octet-stream and the hint specialises it, so the
                    // effective type matches what AutoDetectParser dispatches on
                    detectionMd.set(org.apache.tika.metadata.HttpHeaders.CONTENT_TYPE, httpCTHint);
                }
                // pass the filename so detection matches what the parser dispatches on;
                // without it, an ambiguous byte sequence (e.g. plain text with a .html
                // extension) can resolve differently here than at parse time
                try {
                    URL _url = URLUtil.toURL(url);
                    detectionMd.set(TikaCoreProperties.RESOURCE_NAME_KEY, _url.getFile());
                } catch (MalformedURLException e1) {
                    throw new IllegalStateException("Malformed URL", e1);
                }
                try (TikaInputStream tis = TikaInputStream.get(content)) {
                    mimeType = tika.detect(tis, detectionMd);
                } catch (IOException e) {
                    LOG.warn("Failed to detect MIME type for {}: {}", url, e.getMessage());
                }
                if (mimeType != null) {
                    // write back for the rejected-tuple path only: AutoDetectParser
                    // re-detects on a successful parse and the copy loop overwrites this;
                    // the value here is only visible when the tuple is failed, useful for
                    // debugging why a document was rejected by the whitelist
                    metadata.setValue("parse.Content-Type", mimeType);
                }
            }
            if (mimeType != null) {
                for (Pattern mt : mimeTypeWhiteList) {
                    if (mt.matcher(mimeType).matches()) {
                        mt_match = true;
                        break;
                    }
                }
            }
            if (!mt_match) {
                handleException(url, null, metadata, tuple, "content type");
                return;
            }
        }

        // the document got trimmed during the fetching - no point in trying to
        // parse it
        if ("true"
                .equalsIgnoreCase(
                        metadata.getFirstValue(
                                ProtocolResponse.TRIMMED_RESPONSE_KEY, this.protocolMDprefix))) {
            handleException(url, null, metadata, tuple, "skipped_trimmed");
            return;
        }

        long start = System.currentTimeMillis();

        org.apache.tika.metadata.Metadata md = new org.apache.tika.metadata.Metadata();

        // provide the mime-type as a clue for guessing
        String httpCT = metadata.getFirstValue(HttpHeaders.CONTENT_TYPE, this.protocolMDprefix);
        if (StringUtils.isNotBlank(httpCT)) {
            // pass content type from server as a clue
            md.set(org.apache.tika.metadata.HttpHeaders.CONTENT_TYPE, httpCT);
        }

        // as well as the filename
        try {
            URL _url = URLUtil.toURL(url);
            md.set(TikaCoreProperties.RESOURCE_NAME_KEY, _url.getFile());
        } catch (MalformedURLException e1) {
            throw new IllegalStateException("Malformed URL", e1);
        }

        LinkContentHandler linkHandler = new LinkContentHandler();
        ContentHandler textHandler = new BodyContentHandler(textMaxLength);
        TeeContentHandler teeHandler = new TeeContentHandler(linkHandler, textHandler);
        // seed the context with the components configured in the
        // "parse-context" section of the Tika configuration
        ParseContext parseContext = createParseContext();

        if (extractEmbedded) {
            parseContext.set(Parser.class, tika.getParser());
        } else {
            // the AutoDetectParser sets itself on the context unless a parser
            // is present, which would parse the embedded documents anyway
            parseContext.set(Parser.class, EmptyParser.INSTANCE);
        }

        try {
            parseContext.set(
                    HtmlMapper.class, InitialisationUtil.initializeFromClass(htmlMapperClass));
        } catch (Exception e) {
            LOG.error("Exception while specifying HTMLMapper {}", url, e);
        }

        // build a DOM if required by the parseFilters
        DocumentFragment root = null;
        if (parseFilters.needsDOM()) {
            HTMLDocumentImpl doc = new HTMLDocumentImpl();
            doc.setErrorChecking(false);
            root = doc.createDocumentFragment();
            DOMBuilder domhandler = new DOMBuilder(doc, root);
            domhandler.setUpperCaseElementNames(upperCaseElementNames);
            domhandler.setDefaultNamespaceURI(XHTMLContentHandler.XHTML);
            teeHandler = new TeeContentHandler(linkHandler, textHandler, domhandler);
        }

        // parse
        String text;
        boolean textTrimmed = false;
        try (TikaInputStream tis = TikaInputStream.get(content)) {
            parseWithTimeout(tis, teeHandler, md, parseContext);
            text = textHandler.toString();
        } catch (TimeoutException e) {
            handleException(url, null, metadata, tuple, "parse timeout");
            return;
        } catch (Throwable e) {
            if (!WriteLimitReachedException.isWriteLimitReached(e)) {
                handleException(url, e, metadata, tuple, "parse error");
                return;
            }
            // the parse stopped at the text limit: keep the text and the
            // links extracted so far instead of failing the document
            text = textHandler.toString();
            textTrimmed = true;
            LOG.info("Text of {} trimmed to {} characters", url, textMaxLength);
            eventCounter.scope("text_trimmed").incrBy(1);
        }

        // add parse md to metadata
        for (String k : md.names()) {
            String[] values = md.getValues(k);
            metadata.setValues("parse." + k, values);
        }

        if (textTrimmed) {
            metadata.setValue(TEXT_TRIMMED_KEY, "true");
        }

        long duration = System.currentTimeMillis() - start;

        LOG.info("Parsed {} in {} msec", url, duration);

        // filter and convert the outlinks
        List<Outlink> outlinks = toOutlinks(url, linkHandler.getLinks(), metadata);

        ParseResult parse = new ParseResult(outlinks);

        // parse data of the parent URL
        ParseData parseData = parse.getOrCreate(url);
        parseData.setMetadata(metadata);
        parseData.setText(text);
        parseData.setContent(content);

        // apply the parse filters if any
        try {
            parseFilters.filter(url, content, root, parse);
        } catch (RuntimeException e) {
            handleException(url, e, metadata, tuple, "parse filters");
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

        // emit each document/subdocument in the ParseResult object;
        // skip empty entries for other URLs, which generally stem from a
        // lookup on a URL which was never parsed
        for (Map.Entry<String, ParseData> doc : parse) {
            ParseData parseDoc = doc.getValue();
            if (!doc.getKey().equals(url) && isEmptyDocument(parseDoc)) {
                LOG.debug("Skipping empty ParseData for {}", doc.getKey());
                eventCounter.scope("skipped_empty_documents").incrBy(1);
                continue;
            }

            collector.emit(
                    tuple,
                    new Values(
                            doc.getKey(),
                            parseDoc.getContent(),
                            parseDoc.getMetadata(),
                            parseDoc.getText()));
        }

        collector.ack(tuple);
        eventCounter.scope("tuple_success").incrBy(1);
    }

    /**
     * Parses the document on the executor thread, or on a separate thread under {@link
     * #PARSE_TIMEOUT_PARAM} if a timeout is set.
     *
     * @throws TimeoutException if the parse did not complete in time
     */
    private void parseWithTimeout(
            TikaInputStream tis,
            ContentHandler handler,
            org.apache.tika.metadata.Metadata md,
            ParseContext parseContext)
            throws Exception {
        if (parseExecutor == null) {
            parse(tis, handler, md, parseContext);
            return;
        }
        Future<?> future =
                parseExecutor.submit(
                        () -> {
                            parse(tis, new InterruptibleContentHandler(handler), md, parseContext);
                            return null;
                        });
        try {
            future.get(parseTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw e;
        } catch (TimeoutException e) {
            // parsers rarely check for interrupts; the handler throws at the next
            // SAX event but a parser stuck without producing output keeps its thread,
            // so the next documents get a new one
            future.cancel(true);
            parseExecutor.shutdownNow();
            parseExecutor = newParseExecutor();
            throw e;
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    /** Parses the document with the Tika parser. Overridden in tests. */
    void parse(
            TikaInputStream tis,
            ContentHandler handler,
            org.apache.tika.metadata.Metadata md,
            ParseContext parseContext)
            throws Exception {
        tika.getParser().parse(tis, handler, md, parseContext);
    }

    private static ExecutorService newParseExecutor() {
        return Executors.newSingleThreadExecutor(
                r -> {
                    Thread t = new Thread(r, "tika-parse-" + PARSE_THREAD_COUNT.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                });
    }

    /** Stops the parse at the next SAX event once the parsing thread has been interrupted. */
    private static class InterruptibleContentHandler extends ContentHandlerDecorator {

        InterruptibleContentHandler(ContentHandler handler) {
            super(handler);
        }

        private static void checkInterrupted() throws SAXException {
            if (Thread.currentThread().isInterrupted()) {
                throw new SAXException("Parse interrupted");
            }
        }

        @Override
        public void startElement(String uri, String localName, String name, Attributes atts)
                throws SAXException {
            checkInterrupted();
            super.startElement(uri, localName, name, atts);
        }

        @Override
        public void endElement(String uri, String localName, String name) throws SAXException {
            checkInterrupted();
            super.endElement(uri, localName, name);
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            checkInterrupted();
            super.characters(ch, start, length);
        }

        @Override
        public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
            checkInterrupted();
            super.ignorableWhitespace(ch, start, length);
        }
    }

    private static boolean isEmptyDocument(ParseData parseDoc) {
        byte[] content = parseDoc.getContent();
        return (content == null || content.length == 0)
                && StringUtils.isBlank(parseDoc.getText())
                && (parseDoc.getMetadata() == null || parseDoc.getMetadata().size() == 0);
    }

    private Tika instantiateTika(Map<String, Object> conf) {
        String tikaConfigFile =
                ConfUtils.getString(conf, "parser.tika.config.file", "tika-config.json");
        long start = System.currentTimeMillis();
        URL tikaConfigUrl = getClass().getClassLoader().getResource(tikaConfigFile);
        if (tikaConfigUrl == null) {
            // fail fast: silently falling back to the default configuration
            // would activate parsers the configuration excluded
            throw new IllegalStateException(
                    "Tika configuration file " + tikaConfigFile + " not found on classpath");
        }
        LOG.info("Instantiating Tika using custom configuration {}", tikaConfigUrl);
        Path configPath = null;
        boolean temporary = false;
        try {
            if ("file".equals(tikaConfigUrl.getProtocol())) {
                configPath = Paths.get(tikaConfigUrl.toURI());
            } else {
                // TikaLoader can only read configurations from the filesystem:
                // copy the resource to a temporary file and delete it as soon
                // as the configuration has been loaded
                configPath = Files.createTempFile("tika-config", ".json");
                temporary = true;
                try (InputStream is = tikaConfigUrl.openStream()) {
                    Files.copy(is, configPath, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            TikaLoader tikaLoader = TikaLoader.load(configPath, getClass().getClassLoader());
            configuredParseContext = tikaLoader.loadParseContext();
            Tika tika = new Tika(tikaLoader.loadDetectors(), tikaLoader.loadAutoDetectParser());
            LOG.debug("Tika loaded in {} msec", System.currentTimeMillis() - start);
            return tika;
        } catch (IOException | TikaConfigException | URISyntaxException e) {
            throw new IllegalStateException(
                    "Failed to instantiate Tika using custom configuration " + tikaConfigUrl, e);
        } finally {
            if (temporary && configPath != null) {
                try {
                    Files.deleteIfExists(configPath);
                } catch (IOException e) {
                    LOG.warn("Failed to delete temporary Tika configuration {}", configPath, e);
                }
            }
        }
    }

    /**
     * Returns a ParseContext seeded with the components configured in the "parse-context" section
     * of the Tika configuration.
     */
    ParseContext createParseContext() {
        ParseContext parseContext = new ParseContext();
        parseContext.copyFrom(configuredParseContext);
        return parseContext;
    }

    /** Returns the Tika instance used by this bolt. Exposed for tests. */
    Tika getTika() {
        return tika;
    }

    private void handleException(
            String url, Throwable e, Metadata metadata, Tuple tuple, String errorType) {
        // real exception?
        if (e != null) {
            LOG.error("{} -> {}", errorType, url, e);
        } else {
            LOG.info("{} -> {}", errorType, url);
        }
        metadata.setValue(Constants.STATUS_ERROR_SOURCE, "TIKA");
        metadata.setValue(Constants.STATUS_ERROR_MESSAGE, errorType);
        collector.emit(StatusStreamName, tuple, new Values(url, metadata, Status.ERROR));
        collector.ack(tuple);
        // Increment metric that is context specific
        String s = "error_" + errorType.replaceAll(" ", "_");
        eventCounter.scope(s).incrBy(1);
        // Increment general metric
        eventCounter.scope("parse exception").incrBy(1);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "content", "metadata", "text"));
        declarer.declareStream(StatusStreamName, new Fields("url", "metadata", "status"));
    }

    private List<Outlink> toOutlinks(String parentURL, List<Link> links, Metadata parentMetadata) {

        Map<String, Outlink> outlinks = new HashMap<>();

        URL url_;
        try {
            url_ = new URL(parentURL);
        } catch (MalformedURLException e1) {
            // we would have known by now as previous
            // components check whether the URL is valid
            LOG.error("MalformedURLException on {}", parentURL);
            eventCounter.scope("error_invalid_source_url").incrBy(1);
            return new ArrayList<>();
        }

        for (Link l : links) {
            if (StringUtils.isBlank(l.getUri())) {
                continue;
            }
            String urlOL;

            // build an absolute URL
            try {
                URL tmpURL = URLUtil.resolveUrl(url_, l.getUri());
                urlOL = tmpURL.toExternalForm();
            } catch (MalformedURLException e) {
                LOG.debug("MalformedURLException on {}", l.getUri());
                eventCounter
                        .scope("error_outlink_parsing_" + e.getClass().getSimpleName())
                        .incrBy(1);
                continue;
            }

            // applies the URL filters
            if (urlFilters != null) {
                urlOL = urlFilters.filter(url_, parentMetadata, urlOL);
                if (urlOL == null) {
                    eventCounter.scope("outlink_filtered").incrBy(1);
                    continue;
                }
            }

            eventCounter.scope("outlink_kept").incrBy(1);

            Outlink ol = new Outlink(urlOL);
            // add the anchor
            ol.setAnchor(l.getText());

            // get the metadata for the outlink from the parent ones
            ol.setMetadata(metadataTransfer.getMetaForOutlink(urlOL, parentURL, parentMetadata));

            // keep only one instance of outlink per URL
            outlinks.putIfAbsent(urlOL, ol);
        }
        return new ArrayList<>(outlinks.values());
    }

    @Override
    public void cleanup() {
        if (parseExecutor != null) {
            parseExecutor.shutdownNow();
        }
        if (parseFilters != null) {
            parseFilters.cleanup();
        }
    }
}
