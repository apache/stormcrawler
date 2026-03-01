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

package org.apache.stormcrawler.protocol.playwright;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.Browser.NewContextOptions;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.Playwright.CreateOptions;
import com.microsoft.playwright.Tracing;
import com.microsoft.playwright.options.Proxy;
import com.microsoft.playwright.options.WaitUntilState;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.utils.MutableInt;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.AbstractHttpProtocol;
import org.apache.stormcrawler.protocol.Protocol;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.apache.stormcrawler.util.ConfUtils;
import org.slf4j.LoggerFactory;

public class HttpProtocol extends AbstractHttpProtocol {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HttpProtocol.class);

    private Browser browser;

    public static final String MD_KEY_START = "playwright.protocol.start";
    public static final String MD_KEY_END = "playwright.protocol.end";
    public static final String MD_TRACE = "playwright.trace";
    public static final String MD_EVALUATIONS = "playwright.evaluations";
    public static final String MD_SKIPS = "playwright.skip.resource.types";

    private int timeout = 10000;

    private BrowserContext context;

    private Set<String> resourceTypesToSkip;

    private ObjectMapper mapper;

    // The Page.evaluate() API can run a JavaScript function in the context of the
    // web page and bring results back to the Playwright environment. Browser
    // globals like window and document can be used in evaluate.
    private List<String> evaluations;

    private WaitUntilState loadEvent;

    @Override
    public void configure(final Config conf) {
        super.configure(conf);

        // "http://localhost:9222"
        final String CDP_URL = ConfUtils.getString(conf, "playwright.cdp.url");

        // "ws://localhost:3000/"
        final String REMOTE_WS = ConfUtils.getString(conf, "playwright.remote.ws");

        boolean skipDownloads = ConfUtils.getBoolean(conf, "playwright.skip.download", false);

        String loadEventString = ConfUtils.getString(conf, "playwright.load.event", "load");
        // map load event string to playwright native wait stat
        switch (loadEventString) {
            case "domcontentloaded":
                loadEvent = WaitUntilState.DOMCONTENTLOADED;
                break;
            case "networkidle":
                loadEvent = WaitUntilState.NETWORKIDLE;
                break;
            default:
                loadEvent = WaitUntilState.LOAD;
                break;
        }

        final CreateOptions creationOptions = new CreateOptions();
        final Map<String, String> env = new HashMap<>();

        // no need to download if we are connecting to a remote instance
        if (StringUtils.isNotBlank(CDP_URL) || StringUtils.isNotBlank(REMOTE_WS)) {
            skipDownloads = true;
        }

        // skip browser download if relying on an existing one
        if (skipDownloads) {
            env.put("PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD", "true");
        }

        creationOptions.setEnv(env);

        BrowserType btype = Playwright.create(creationOptions).chromium();
        if (StringUtils.isNotBlank(CDP_URL) && StringUtils.isNotBlank(REMOTE_WS)) {
            throw new RuntimeException(
                    "Can't specify both cdp.url and remote.ws in the configuration");
        } else if (StringUtils.isNotBlank(CDP_URL)) {
            browser = btype.connectOverCDP(CDP_URL);
        } else if (StringUtils.isNotBlank(REMOTE_WS)) {
            browser = btype.connect(REMOTE_WS);
        } else {
            browser = btype.launch();
        }

        timeout = ConfUtils.getInt(conf, "http.timeout", timeout);

        final String ua = getAgentString(conf);

        NewContextOptions b_c_options =
                new Browser.NewContextOptions().setIsMobile(false).setUserAgent(ua);

        // global proxy
        String proxyServer = ConfUtils.getString(conf, "http.proxy");
        String proxyUser = ConfUtils.getString(conf, "http.proxy.username");
        String proxyPwd = ConfUtils.getString(conf, "http.proxy.password");

        final Proxy globalProxy = getProxy(proxyServer, proxyUser, proxyPwd);
        if (globalProxy != null) {
            b_c_options.setProxy(globalProxy);
            b_c_options.setIgnoreHTTPSErrors(true);
        }

        context = browser.newContext(b_c_options);

        context.setDefaultTimeout(timeout);

        // list of resource types to skip
        // document, stylesheet, image, media, font, script, texttrack, xhr, fetch,
        // eventsource, websocket, manifest, other
        resourceTypesToSkip = new java.util.HashSet<>();
        for (String v : ConfUtils.loadListFromConf(MD_SKIPS, conf)) {
            resourceTypesToSkip.add(v);
        }

        mapper = new ObjectMapper();

        // expressions to evaluate
        evaluations = ConfUtils.loadListFromConf(MD_EVALUATIONS, conf);
    }

    @Override
    public ProtocolResponse getProtocolOutput(String url, Metadata md) throws Exception {

        // https://github.com/microsoft/playwright-java#is-playwright-thread-safe
        synchronized (this) {

            // tracing// Start tracing before creating / navigating a page.
            if (md.containsKey(MD_TRACE)) {
                context.tracing()
                        .start(
                                new Tracing.StartOptions()
                                        .setScreenshots(false)
                                        .setSnapshots(true)
                                        .setSources(true));
            }

            final Metadata responseMetaData = new Metadata();
            responseMetaData.addValue(MD_KEY_START, Instant.now().toString());

            final MutableInt status = new MutableInt(-1);
            byte[] content = new byte[0];

            try (Page page = context.newPage()) {

                page.onResponse(
                        response -> {
                            // make sure that this applies to the main page
                            if (response.url().equals(url)) {
                                // redirection?
                                if (Status.REDIRECTION.equals(
                                        Status.fromHTTPCode(response.status()))) {
                                    status.set(response.status());
                                    response.allHeaders()
                                            .forEach(
                                                    (k, v) -> {
                                                        responseMetaData.addValue(k, v);
                                                    });
                                }
                            }
                        });

                page.onPageError(
                        handler -> {
                            // this applies to any resource - not just the main page
                            LOG.debug("Error when loading {} {}", url, handler);
                        });

                // NOTE: The handler will only be called for the first url if the
                // response is a redirect.
                page.route(
                        _url -> true,
                        route -> {
                            // abort if we know the main page is a redirection
                            if (status.get() != -1) {
                                LOG.debug("Aborting request for {}", route.request().url());
                                route.abort();
                            } else if (resourceTypesToSkip.contains(
                                    route.request().resourceType())) {
                                route.abort();
                            } else {
                                route.resume();
                            }
                        });

                // let playwright do the content loading
                com.microsoft.playwright.Response response =
                        page.navigate(
                                url,
                                new Page.NavigateOptions()
                                        .setTimeout(timeout)
                                        .setWaitUntil(loadEvent));

                // the status is not set unless
                // a redirection
                if (status.get() == -1) {
                    response.allHeaders()
                            .forEach(
                                    (k, v) -> {
                                        responseMetaData.addValue(k, v);
                                    });

                    if (Status.FETCHED == Status.fromHTTPCode(response.status())) {
                        // retrieve the rendered content
                        content = page.content().getBytes(StandardCharsets.UTF_8);
                    }

                    status.set(response.status());

                    // evaluate an expression and store the results
                    // in the metadata using the same string as key
                    for (String expression : evaluations) {
                        Object performance = page.evaluate(expression);
                        if (performance != null) {
                            String json =
                                    mapper.writerWithDefaultPrettyPrinter()
                                            .writeValueAsString(performance);
                            responseMetaData.setValue(expression, json);
                        }
                    }
                }
            }

            if (md.containsKey(MD_TRACE)) {
                Path tmp = Files.createTempFile("trace-", ".zip", new FileAttribute[0]);
                context.tracing().stop(new Tracing.StopOptions().setPath(tmp));
                responseMetaData.setValue(MD_TRACE, tmp.toString());
            }

            responseMetaData.addValue(MD_KEY_END, Instant.now().toString());

            return new ProtocolResponse(content, status.get(), responseMetaData);
        }
    }

    /** Returns a proxy object if required * */
    private Proxy getProxy(String proxyserver, String proxyuser, String proxypwd) {
        if (proxyserver == null) {
            return null;
        }

        Proxy proxy = new Proxy(proxyserver);
        if (proxyuser != null) {
            proxy.setUsername(proxyuser);
        }
        if (proxypwd != null) {
            proxy.setPassword(proxypwd);
        }
        return proxy;
    }

    @Override
    public void cleanup() {
        synchronized (this) {
            super.cleanup();
            context.close();
            browser.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Protocol.main(new HttpProtocol(), args);
    }
}
