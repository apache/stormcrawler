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

package org.apache.stormcrawler.protocol;

import crawlercommons.robots.BaseRobotRules;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.Config;
import org.apache.stormcrawler.proxy.ProxyManager;
import org.apache.stormcrawler.util.ConfUtils;
import org.apache.stormcrawler.util.InitialisationUtil;
import org.slf4j.LoggerFactory;

public abstract class AbstractHttpProtocol implements Protocol {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AbstractHttpProtocol.class);

    /**
     * Formatter for dates in HTTP headers, used to fill the &quot;If-Modified-Since&quot; request
     * header field, e.g.
     *
     * <pre>
     * Sun, 06 Nov 1994 08:49:37 GMT
     * </pre>
     *
     * See <a href= "https://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.3.1">sec. 3.3.1 in
     * RFC 2616</a> and <a href="https://tools.ietf.org/html/rfc7231#section-7.1.1.1">sec. 7.1.1.1
     * in RFC 7231</a>. The latter specifies the format defined in RFC 1123 as the
     * &quot;preferred&quot; format.
     */
    private static final DateTimeFormatter HTTP_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.ROOT)
                    .withZone(ZoneId.of(ZoneOffset.UTC.toString()));

    /** Formatter to parse ISO-formatted dates persisted in status index. */
    private static final DateTimeFormatter ISO_INSTANT_FORMATTER =
            DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of(ZoneOffset.UTC.toString()));

    private org.apache.stormcrawler.protocol.HttpRobotRulesParser robots;

    protected boolean skipRobots = false;

    protected boolean storeHttpHeaders = false;

    protected boolean useCookies = false;

    protected List<String> protocolVersions;

    protected static final String RESPONSE_COOKIES_HEADER = "set-cookie";

    protected static final String SET_HEADER_BY_REQUEST = "set-header";

    protected String protocolMetadataPrefix = "";

    public ProxyManager proxyManager;

    protected final List<KeyValue> customHeaders = new LinkedList<>();

    protected static class KeyValue {
        private final String key;
        private final String value;

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public KeyValue(String k, String v) {
            super();
            this.key = k;
            this.value = v;
        }

        public static KeyValue build(String h) {
            int pos = h.indexOf("=");
            if (pos == -1) {
                return new KeyValue(h.trim(), "");
            }
            if (pos + 1 == h.length()) {
                return new KeyValue(h.trim(), "");
            }
            return new KeyValue(h.substring(0, pos).trim(), h.substring(pos + 1).trim());
        }
    }

    @Override
    public void configure(Config conf) {
        this.skipRobots = ConfUtils.getBoolean(conf, "http.robots.file.skip", false);

        this.storeHttpHeaders = ConfUtils.getBoolean(conf, "http.store.headers", false);
        this.useCookies = ConfUtils.getBoolean(conf, "http.use.cookies", false);
        this.protocolVersions = ConfUtils.loadListFromConf("http.protocol.versions", conf);

        List<String> headers = ConfUtils.loadListFromConf("http.custom.headers", conf);
        for (String h : headers) {
            customHeaders.add(KeyValue.build(h));
        }

        robots = new HttpRobotRulesParser(conf);
        protocolMetadataPrefix =
                ConfUtils.getString(
                        conf, ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, protocolMetadataPrefix);

        String proxyManagerImplementation =
                ConfUtils.getString(
                        conf,
                        "http.proxy.manager",
                        // determine whether to set default as
                        // SingleProxyManager by
                        // checking whether legacy proxy field is set
                        (ConfUtils.getString(conf, "http.proxy.host", null) != null)
                                ? "org.apache.stormcrawler.proxy.SingleProxyManager"
                                : null);

        // conditionally load proxy manager
        if (proxyManagerImplementation != null) {
            try {
                // create new proxy manager from file
                proxyManager =
                        InitialisationUtil.initializeFromQualifiedName(
                                proxyManagerImplementation, ProxyManager.class);
                proxyManager.configure(conf);
            } catch (Exception e) {
                LOG.error("Failed to create proxy manager `" + proxyManagerImplementation + "`", e);
            }
        }
    }

    @Override
    public BaseRobotRules getRobotRules(String url) {
        if (this.skipRobots) {
            return RobotRulesParser.EMPTY_RULES;
        }
        return robots.getRobotRulesSet(this, url);
    }

    @Override
    public void cleanup() {}

    /**
     * Build the user agent from the configuration. Used by the protocol implementation to build the
     * requests
     *
     * @return full user agent
     */
    public static String getAgentString(Config conf) {
        String agent = ConfUtils.getString(conf, "http.agent");
        if (agent != null && !agent.isEmpty()) {
            return agent;
        }
        return getAgentString(
                ConfUtils.getString(conf, "http.agent.name"),
                ConfUtils.getString(conf, "http.agent.version"),
                ConfUtils.getString(conf, "http.agent.description"),
                ConfUtils.getString(conf, "http.agent.url"),
                ConfUtils.getString(conf, "http.agent.email"));
    }

    private static String getAgentString(
            String agentName,
            String agentVersion,
            String agentDesc,
            String agentUrl,
            String agentEmail) {

        StringBuilder buf = new StringBuilder();

        buf.append(agentName);

        if (StringUtils.isNotBlank(agentVersion)) {
            buf.append("/");
            buf.append(agentVersion);
        }

        boolean hasAgentDesc = StringUtils.isNotBlank(agentDesc);
        boolean hasAgentUrl = StringUtils.isNotBlank(agentUrl);
        boolean hasAgentEmail = StringUtils.isNotBlank(agentEmail);

        if (hasAgentDesc || hasAgentEmail || hasAgentUrl) {
            buf.append(" (");

            if (hasAgentDesc) {
                buf.append(agentDesc);
                if (hasAgentUrl || hasAgentEmail) {
                    buf.append("; ");
                }
            }

            if (hasAgentUrl) {
                buf.append(agentUrl);
                if (hasAgentEmail) {
                    buf.append("; ");
                }
            }

            if (hasAgentEmail) {
                buf.append(agentEmail);
            }

            buf.append(")");
        }

        return buf.toString();
    }

    /**
     * Format an ISO date string as HTTP date used in HTTP headers. E.g.,
     *
     * <pre>
     * 1994-11-06T08:49:37.000Z
     * </pre>
     *
     * is formatted to
     *
     * <pre>
     * Sun, 06 Nov 1994 08:49:37 GMT
     * </pre>
     *
     * See {@link #HTTP_DATE_FORMATTER}
     */
    protected static String formatHttpDate(String isoDate) {
        try {
            ZonedDateTime date = ISO_INSTANT_FORMATTER.parse(isoDate, ZonedDateTime::from);
            return HTTP_DATE_FORMATTER.format(date);
        } catch (DateTimeParseException e) {
            // not an ISO date
            return "";
        }
    }
}
