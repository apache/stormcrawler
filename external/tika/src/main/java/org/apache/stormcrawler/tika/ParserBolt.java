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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.http.HttpHeaders;
import org.apache.storm.metric.api.MultiCountMetric;
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
import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.html.IdentityHtmlMapper;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.Link;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.xml.sax.ContentHandler;

/** Uses Tika to parse the output of a fetch and extract text + metadata */
public class ParserBolt extends BaseRichBolt {

    private Tika tika;

    private URLFilters urlFilters = null;
    private ParseFilter parseFilters = null;

    private OutputCollector collector;

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ParserBolt.class);

    private MultiCountMetric eventCounter;

    private boolean upperCaseElementNames = true;
    private Class<? extends HtmlMapper> htmlMapperClass = IdentityHtmlMapper.class;

    private boolean extractEmbedded = false;

    private MetadataTransfer metadataTransfer;
    private boolean emitOutlinks = true;

    /** regular expressions to apply to the mime-type * */
    private List<Pattern> mimeTypeWhiteList = new LinkedList<>();

    private String protocolMDprefix;

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

        tika = instantiateTika(conf);

        this.collector = collector;

        this.eventCounter =
                context.registerMetric(this.getClass().getSimpleName(), new MultiCountMetric(), 10);

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
            // see if a mimetype was guessed in JSOUPBolt
            String mimeType = metadata.getFirstValue("parse.Content-Type");
            // otherwise rely on what could have been obtained from HTTP
            if (mimeType == null) {
                mimeType = metadata.getFirstValue(HttpHeaders.CONTENT_TYPE, this.protocolMDprefix);
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

        ByteArrayInputStream bais = new ByteArrayInputStream(content);
        org.apache.tika.metadata.Metadata md = new org.apache.tika.metadata.Metadata();

        // provide the mime-type as a clue for guessing
        String httpCT = metadata.getFirstValue(HttpHeaders.CONTENT_TYPE, this.protocolMDprefix);
        if (StringUtils.isNotBlank(httpCT)) {
            // pass content type from server as a clue
            md.set(org.apache.tika.metadata.Metadata.CONTENT_TYPE, httpCT);
        }

        // as well as the filename
        try {
            URL _url = new URL(url);
            md.set(TikaCoreProperties.RESOURCE_NAME_KEY, _url.getFile());
        } catch (MalformedURLException e1) {
            throw new IllegalStateException("Malformed URL", e1);
        }

        LinkContentHandler linkHandler = new LinkContentHandler();
        ContentHandler textHandler = new BodyContentHandler(-1);
        TeeContentHandler teeHandler = new TeeContentHandler(linkHandler, textHandler);
        ParseContext parseContext = new ParseContext();

        if (extractEmbedded) {
            parseContext.set(Parser.class, tika.getParser());
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
        try {
            tika.getParser().parse(bais, teeHandler, md, parseContext);
            text = textHandler.toString();
        } catch (Throwable e) {
            handleException(url, e, metadata, tuple, "parse error");
            return;
        } finally {
            try {
                bais.close();
            } catch (IOException e) {
                LOG.error("Exception while closing stream", e);
            }
        }

        // add parse md to metadata
        for (String k : md.names()) {
            String[] values = md.getValues(k);
            metadata.setValues("parse." + k, values);
        }

        long duration = System.currentTimeMillis() - start;

        LOG.info("Parsed {} in {} msec", url, duration);

        // filter and convert the outlinks
        List<Outlink> outlinks = toOutlinks(url, linkHandler.getLinks(), metadata);

        ParseResult parse = new ParseResult(outlinks);

        // parse data of the parent URL
        ParseData parseData = parse.get(url);
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

        collector.ack(tuple);
        eventCounter.scope("tuple_success").incrBy(1);
    }

    private Tika instantiateTika(Map<String, Object> conf) {
        Tika tika = null;
        String tikaConfigFile =
                ConfUtils.getString(conf, "parser.tika.config.file", "tika-config.xml");
        long start = System.currentTimeMillis();
        URL tikaConfigUrl = getClass().getClassLoader().getResource(tikaConfigFile);
        if (tikaConfigUrl == null) {
            LOG.error("Tika configuration file {} not found on classpath", tikaConfigFile);
        } else {
            LOG.info("Instantiating Tika using custom configuration {}", tikaConfigUrl);
            try {
                TikaConfig tikaConfig = new TikaConfig(tikaConfigUrl, getClass().getClassLoader());
                tika = new Tika(tikaConfig);
            } catch (Exception e) {
                LOG.error(
                        "Failed to instantiate Tika using custom configuration {}",
                        tikaConfigUrl,
                        e);
            }
        }
        if (tika == null) {
            LOG.info("Instantiating Tika with default configuration");
            tika = new Tika();
        }
        long end = System.currentTimeMillis();
        LOG.debug("Tika loaded in {} msec", end - start);
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
    }
}
