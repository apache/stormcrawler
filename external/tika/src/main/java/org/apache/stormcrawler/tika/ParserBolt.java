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
import java.io.StringReader;
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
import java.util.regex.Pattern;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
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
import org.apache.tika.config.TimeoutLimits;
import org.apache.tika.config.loader.TikaLoader;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.WriteLimitReachedException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.EmptyParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.DefaultHtmlMapper;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.html.IdentityHtmlMapper;
import org.apache.tika.pipes.api.ParseMode;
import org.apache.tika.pipes.api.PipesResult;
import org.apache.tika.pipes.core.PipesException;
import org.apache.tika.pipes.fork.PipesForkParser;
import org.apache.tika.pipes.fork.PipesForkParserConfig;
import org.apache.tika.pipes.fork.PipesForkResult;
import org.apache.tika.sax.BasicContentHandlerFactory;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.Link;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;

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
     * Configuration key for the maximum time in milliseconds a document may take to parse, 0 or
     * less for no limit. Runs the parse in a forked JVM (Tika Pipes) and kills it outright if
     * exceeded. Keep below {@code topology.message.timeout.secs}.
     */
    public static final String PARSE_TIMEOUT_PARAM = "parser.tika.timeout";

    /**
     * Directory holding Tika Pipes plugin zips under {@link #PARSE_TIMEOUT_PARAM}, not needed by
     * default. Unset uses Tika's default.
     */
    public static final String PIPES_PLUGINS_DIR_PARAM = "parser.tika.pipes.plugins.dir";

    /** JVM arguments passed to each forked process under {@link #PARSE_TIMEOUT_PARAM}. */
    public static final String PIPES_JVM_ARGS_PARAM = "parser.tika.pipes.jvmargs";

    /** Restart a forked process after this many documents under {@link #PARSE_TIMEOUT_PARAM}. */
    public static final String PIPES_MAX_FILES_PER_PROCESS_PARAM =
            "parser.tika.pipes.maxfilesperprocess";

    /**
     * Cap on the characters of text a fork may return when {@link #TEXT_MAX_LENGTH_PARAM} is not
     * set. Tika counts text only, the markup around it comes on top.
     */
    private static final int PIPES_WRITE_LIMIT_CHARS = 20_000_000;

    private static final SAXParserFactory PIPES_CONTENT_PARSER_FACTORY =
            newHardenedSaxParserFactory();

    private static SAXParserFactory newHardenedSaxParserFactory() {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(true);
        try {
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        } catch (ParserConfigurationException | SAXException e) {
            throw new IllegalStateException(
                    "Failed to configure the XML parser used to re-read Tika Pipes output", e);
        }
        return factory;
    }

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

    /** Runs parses under {@link #PARSE_TIMEOUT_PARAM} in a forked JVM; null otherwise. */
    private PipesForkParser pipesForkParser;

    /** On-disk copy of the Tika configuration, kept alive to merge into the forked JVM's config. */
    private Path resolvedTikaConfigPath;

    /** Whether {@link #resolvedTikaConfigPath} is a temp copy this bolt owns and must delete. */
    private boolean resolvedTikaConfigPathIsTemporary;

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

        tika = instantiateTika(conf);

        if (parseTimeout > 0) {
            pipesForkParser = buildPipesForkParser(conf);
        } else {
            deleteTemporaryTikaConfig();
        }

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
            if (pipesForkParser != null) {
                PipesParseOutcome outcome = parseWithPipes(tis, md, teeHandler, url);
                md = outcome.metadata();
                textTrimmed = outcome.trimmed();
                if (textTrimmed) {
                    LOG.info("Text of {} trimmed", url);
                    eventCounter.scope("text_trimmed").incrBy(1);
                }
            } else {
                tika.getParser().parse(tis, teeHandler, md, parseContext);
            }
            text = textHandler.toString();
        } catch (ParseTimeoutException e) {
            handleException(url, null, metadata, tuple, "parse timeout");
            return;
        } catch (ParseCrashException e) {
            handleException(url, e, metadata, tuple, "parse crash");
            return;
        } catch (ParsePipesInfraException e) {
            // infra problem, not a bad document -- affects every parse, distinct status
            handleException(url, e, metadata, tuple, "parse pipes error");
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
        Path configPath;
        try {
            if ("file".equals(tikaConfigUrl.getProtocol())) {
                configPath = Paths.get(tikaConfigUrl.toURI());
            } else {
                // TikaLoader needs a filesystem path; kept until cleanup() when the forked
                // JVMs under parser.tika.timeout need it too, deleteOnExit() as a safety net
                configPath = Files.createTempFile("tika-config", ".json");
                configPath.toFile().deleteOnExit();
                resolvedTikaConfigPathIsTemporary = true;
                try (InputStream is = tikaConfigUrl.openStream()) {
                    Files.copy(is, configPath, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            resolvedTikaConfigPath = configPath;
            TikaLoader tikaLoader = TikaLoader.load(configPath, getClass().getClassLoader());
            configuredParseContext = tikaLoader.loadParseContext();
            Tika tika = new Tika(tikaLoader.loadDetectors(), tikaLoader.loadAutoDetectParser());
            LOG.debug("Tika loaded in {} msec", System.currentTimeMillis() - start);
            return tika;
        } catch (IOException | TikaConfigException | URISyntaxException e) {
            throw new IllegalStateException(
                    "Failed to instantiate Tika using custom configuration " + tikaConfigUrl, e);
        }
    }

    /**
     * Builds the {@link PipesForkParser} used under {@link #PARSE_TIMEOUT_PARAM}: the parent kills
     * the forked process outright via {@code socketTimeoutMillis}, not cooperative interruption.
     */
    private PipesForkParser buildPipesForkParser(Map<String, Object> conf) {
        PipesForkParserConfig pipesConfig = new PipesForkParserConfig();
        // XML, not HTML: ToXMLContentHandler always self-closes/escapes (safe to re-parse
        // locally); ToHTMLContentHandler leaves some elements unclosed per the HTML spec.
        pipesConfig.setHandlerType(BasicContentHandlerFactory.HANDLER_TYPE.XML);
        // stop the fork where the text would be trimmed anyway
        pipesConfig.setWriteLimit(textMaxLength >= 0 ? textMaxLength : PIPES_WRITE_LIMIT_CHARS);
        // matches how the direct parse already merges embedded content into one stream
        pipesConfig.setParseMode(ParseMode.CONCATENATE);
        pipesConfig.setMaxEmbeddedCount(extractEmbedded ? -1 : 0);
        pipesConfig.setTimeoutLimits(new TimeoutLimits(parseTimeout, parseTimeout));
        // the real enforcement: kills the forked process outright if it doesn't respond in time
        pipesConfig.getPipesConfig().setSocketTimeoutMillis(parseTimeout);
        if (resolvedTikaConfigPath != null) {
            pipesConfig.setUserConfigPath(resolvedTikaConfigPath);
        }

        // execute() parses one document at a time, a second fork per bolt would sit idle
        pipesConfig.setNumClients(1);
        // same JVM as the Storm worker instead of whatever "java" is on the PATH
        pipesConfig
                .getPipesConfig()
                .setJavaPath(
                        Paths.get(System.getProperty("java.home"), "bin", "java").toString());
        int maxFilesPerProcess = ConfUtils.getInt(conf, PIPES_MAX_FILES_PER_PROCESS_PARAM, -1);
        if (maxFilesPerProcess > 0) {
            pipesConfig.setMaxFilesPerProcess(maxFilesPerProcess);
        }
        String pluginsDir = ConfUtils.getString(conf, PIPES_PLUGINS_DIR_PARAM, null);
        if (StringUtils.isNotBlank(pluginsDir)) {
            pipesConfig.setPluginsDir(Paths.get(pluginsDir));
        }
        List<String> jvmArgs = ConfUtils.loadListFromConf(PIPES_JVM_ARGS_PARAM, conf);
        if (!jvmArgs.isEmpty()) {
            pipesConfig.setJvmArgs(jvmArgs);
        }

        // an HtmlMapper can't be passed to the forked JVM, which always uses Tika's default
        if (!DefaultHtmlMapper.class.equals(htmlMapperClass)) {
            LOG.warn(
                    "parser.htmlmapper.classname ({}) is ignored under {}: the forked JVM maps"
                            + " HTML with {}, which drops some elements from the DOM given to"
                            + " the parse filters",
                    htmlMapperClass.getName(),
                    PARSE_TIMEOUT_PARAM,
                    DefaultHtmlMapper.class.getSimpleName());
        }

        try {
            return new PipesForkParser(pipesConfig);
        } catch (IOException | TikaConfigException e) {
            throw new IllegalStateException(
                    "Failed to initialise the Tika Pipes fork parser for " + PARSE_TIMEOUT_PARAM,
                    e);
        }
    }

    /**
     * The parse under {@link #PARSE_TIMEOUT_PARAM} did not complete within {@link #parseTimeout}.
     */
    private static final class ParseTimeoutException extends Exception {
        ParseTimeoutException(String message) {
            super(message);
        }
    }

    /**
     * The forked JVM crashed (OOM or otherwise) while parsing; it restarts for the next document.
     */
    private static final class ParseCrashException extends Exception {
        ParseCrashException(String message) {
            super(message);
        }
    }

    /** Tika Pipes itself is misconfigured or unavailable, independently of the document parsed. */
    private static final class ParsePipesInfraException extends Exception {
        ParsePipesInfraException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private record PipesParseOutcome(org.apache.tika.metadata.Metadata metadata, boolean trimmed) {}

    /**
     * Parses {@code tis} in a forked JVM, then replays the returned content into {@code handler} as
     * if parsed in-process, so outlink/text/DOM handling downstream is unchanged.
     */
    private PipesParseOutcome parseWithPipes(
            TikaInputStream tis,
            org.apache.tika.metadata.Metadata seedMetadata,
            ContentHandler handler,
            String url)
            throws ParseTimeoutException,
                    ParseCrashException,
                    ParsePipesInfraException,
                    IOException,
                    SAXException {
        PipesForkResult result;
        try {
            result = pipesForkParser.parse(tis, seedMetadata, new ParseContext());
        } catch (TikaException | PipesException e) {
            throw new ParsePipesInfraException("Tika Pipes error for " + url, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ParsePipesInfraException(
                    "Interrupted while waiting for a Tika Pipes parse of " + url, e);
        }

        if (result.isProcessCrash()) {
            if (result.getStatus() == PipesResult.RESULT_STATUS.TIMEOUT) {
                throw new ParseTimeoutException(
                        "Tika parse of " + url + " exceeded " + parseTimeout + "ms");
            }
            throw new ParseCrashException(
                    "Tika parse of "
                            + url
                            + " crashed the forked process: "
                            + result.getStatus()
                            + (result.getMessage() != null ? " - " + result.getMessage() : ""));
        }

        if (!result.isSuccess()) {
            throw new IOException(
                    "Tika Pipes parse of "
                            + url
                            + " failed: "
                            + result.getStatus()
                            + (result.getMessage() != null ? " - " + result.getMessage() : ""));
        }

        org.apache.tika.metadata.Metadata resultMetadata = result.getMetadata();

        if (result.getStatus() == PipesResult.RESULT_STATUS.PARSE_SUCCESS_WITH_EXCEPTION
                && !isWriteLimitReached(resultMetadata)) {
            // not a text.maxlength trim: match the direct path, which discards the whole
            // document rather than keep partial content
            throw new IOException(
                    "Tika Pipes parse of "
                            + url
                            + " threw: "
                            + (resultMetadata != null
                                    ? resultMetadata.get(TikaCoreProperties.CONTAINER_EXCEPTION)
                                    : result.getMessage()));
        }

        boolean trimmed =
                result.getStatus() == PipesResult.RESULT_STATUS.PARSE_SUCCESS_WITH_EXCEPTION
                        || result.getStatus() == PipesResult.RESULT_STATUS.PARTIAL_TIMEOUT;

        String xml = result.getContent();
        if (StringUtils.isNotBlank(xml)) {
            try {
                reparseIntoHandler(xml, handler);
            } catch (SAXException e) {
                if (WriteLimitReachedException.isWriteLimitReached(e)) {
                    trimmed = true;
                } else if (!(trimmed && e instanceof SAXParseException)) {
                    throw e;
                }
                // a trimmed fork returns XML cut off mid-document: the handlers keep
                // what they got before the cut, as with a trimmed direct parse
            }
        }

        org.apache.tika.metadata.Metadata md =
                resultMetadata != null ? resultMetadata : seedMetadata;
        // the fork returns the content as metadata, it must not end up in parse.*
        md.remove(TikaCoreProperties.TIKA_CONTENT.getName());
        md.remove(TikaCoreProperties.TIKA_CONTENT_HANDLER_TYPE.getName());
        return new PipesParseOutcome(md, trimmed);
    }

    private static boolean isWriteLimitReached(org.apache.tika.metadata.Metadata metadata) {
        return metadata != null
                && "true".equalsIgnoreCase(metadata.get(TikaCoreProperties.WRITE_LIMIT_REACHED));
    }

    /**
     * Re-parses already-extracted XML into {@code handler}. Produced by {@code
     * ToXMLContentHandler}, which always self-closes/escapes, so it's safe to re-read this way.
     */
    private static void reparseIntoHandler(String xml, ContentHandler handler)
            throws SAXException, IOException {
        try {
            XMLReader reader = PIPES_CONTENT_PARSER_FACTORY.newSAXParser().getXMLReader();
            reader.setContentHandler(handler);
            reader.parse(new InputSource(new StringReader(xml)));
        } catch (ParserConfigurationException e) {
            throw new IllegalStateException(
                    "Failed to create the XML parser used to re-read Tika Pipes output", e);
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
        if (parseFilters != null) {
            parseFilters.cleanup();
        }
        if (pipesForkParser != null) {
            try {
                pipesForkParser.close();
            } catch (IOException e) {
                LOG.warn("Failed to close the Tika Pipes fork parser", e);
            }
        }
        deleteTemporaryTikaConfig();
    }

    private void deleteTemporaryTikaConfig() {
        if (resolvedTikaConfigPathIsTemporary && resolvedTikaConfigPath != null) {
            try {
                Files.deleteIfExists(resolvedTikaConfigPath);
            } catch (IOException e) {
                LOG.warn(
                        "Failed to delete temporary Tika configuration {}",
                        resolvedTikaConfigPath,
                        e);
            }
            resolvedTikaConfigPathIsTemporary = false;
        }
    }
}
