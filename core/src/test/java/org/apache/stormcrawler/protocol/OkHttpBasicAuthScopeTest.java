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

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.Config;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.protocol.okhttp.HttpProtocol;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The Authorization header built from http.basicauth.* is only sent to the hosts listed in
 * http.basicauth.hosts. The local server is reached as localhost and as 127.0.0.1, which the
 * protocol treats as two different hosts.
 */
class OkHttpBasicAuthScopeTest extends AbstractProtocolTest {

    private static final String EXPECTED =
            "Basic "
                    + Base64.getEncoder()
                            .encodeToString("wikiuser:wikipass".getBytes(StandardCharsets.UTF_8));

    /** Authorization header seen per requested "host/path", "null" when absent. */
    static final Map<String, String> authorizationSeen = new ConcurrentHashMap<>();

    /** Location the robots.txt of localhost redirects to, none when null. */
    static volatile String robotsRedirect;

    @Override
    protected Handler[] getHandlers() {
        return new Handler[] {
            new AbstractHandler() {
                @Override
                public void handle(
                        String target,
                        Request baseRequest,
                        jakarta.servlet.http.HttpServletRequest request,
                        HttpServletResponse response)
                        throws IOException {
                    baseRequest.setHandled(true);
                    authorizationSeen.put(
                            request.getServerName() + target,
                            String.valueOf(request.getHeader("Authorization")));
                    final String location;
                    if (target.equals("/redirect")) {
                        location = "http://127.0.0.1:" + HTTP_PORT + "/target";
                    } else if (target.equals("/redirect-same-origin")) {
                        location = "/target";
                    } else if (target.equals("/robots.txt")
                            && "localhost".equals(request.getServerName())) {
                        location = robotsRedirect;
                    } else {
                        location = null;
                    }
                    if (location != null) {
                        response.setStatus(HttpServletResponse.SC_MOVED_PERMANENTLY);
                        response.setHeader("Location", location);
                        response.setContentLength(0);
                        response.getOutputStream().close();
                        return;
                    }
                    response.setStatus(HttpServletResponse.SC_OK);
                    response.setContentType("text/plain");
                    final byte[] content = "hello".getBytes(StandardCharsets.UTF_8);
                    response.setContentLength(content.length);
                    try (OutputStream out = response.getOutputStream()) {
                        out.write(content);
                    }
                }
            }
        };
    }

    @BeforeEach
    void reset() {
        authorizationSeen.clear();
        robotsRedirect = null;
    }

    private Config config(Object hosts) {
        final Config conf = new Config();
        conf.put("http.agent.name", "this_is_only_a_test");
        // the local server is cleartext HTTP: opt in so that only the host list decides
        conf.put("http.credentials.allow.insecure", true);
        conf.put("http.basicauth.user", "wikiuser");
        conf.put("http.basicauth.password", "wikipass");
        if (hosts != null) {
            conf.put("http.basicauth.hosts", hosts);
        }
        return conf;
    }

    private HttpProtocol protocol(Config conf) {
        final HttpProtocol protocol = new HttpProtocol();
        protocol.configure(conf);
        return protocol;
    }

    private void fetch(HttpProtocol protocol, String host, String path) throws Exception {
        protocol.getProtocolOutput("http://" + host + ":" + HTTP_PORT + path, new Metadata());
    }

    @Test
    void credentialsOnlyGoToTheListedHost() throws Exception {
        // the listed host is matched regardless of case
        final HttpProtocol protocol = protocol(config("LocalHost"));
        try {
            fetch(protocol, "localhost", "/page.html");
            fetch(protocol, "127.0.0.1", "/page.html");
        } finally {
            protocol.cleanup();
        }
        Assertions.assertEquals(EXPECTED, authorizationSeen.get("localhost/page.html"));
        Assertions.assertEquals(
                "null",
                authorizationSeen.get("127.0.0.1/page.html"),
                "a host which is not listed must not receive the credentials");
    }

    @Test
    void robotsTxtIsScopedLikeAnyOtherFetch() throws Exception {
        final Config conf = config(List.of("localhost"));
        final HttpProtocol protocol = protocol(conf);
        try {
            final HttpRobotRulesParser parser = new HttpRobotRulesParser(conf);
            parser.getRobotRulesSet(protocol, "http://localhost:" + HTTP_PORT + "/page.html");
            parser.getRobotRulesSet(protocol, "http://127.0.0.1:" + HTTP_PORT + "/page.html");
        } finally {
            protocol.cleanup();
        }
        Assertions.assertEquals(EXPECTED, authorizationSeen.get("localhost/robots.txt"));
        Assertions.assertEquals("null", authorizationSeen.get("127.0.0.1/robots.txt"));
    }

    @Test
    void crossHostRobotsTxtRedirectDoesNotCarryTheCredentials() throws Exception {
        robotsRedirect = "http://127.0.0.1:" + HTTP_PORT + "/robots.txt";
        final Config conf = config("localhost");
        conf.put("http.robots.redirect.crossorigin.allow", true);
        final HttpProtocol protocol = protocol(conf);
        try {
            new HttpRobotRulesParser(conf)
                    .getRobotRulesSet(protocol, "http://localhost:" + HTTP_PORT + "/page.html");
        } finally {
            protocol.cleanup();
        }
        Assertions.assertEquals(EXPECTED, authorizationSeen.get("localhost/robots.txt"));
        Assertions.assertEquals(
                "null",
                authorizationSeen.get("127.0.0.1/robots.txt"),
                "the redirect is followed but its target is not a listed host");
    }

    @Test
    void crossHostRedirectDoesNotCarryTheCredentials() throws Exception {
        // regression guard: a change of origin already stripped the credentials before the host
        // list was introduced
        final Config conf = config("localhost");
        conf.put("http.allow.redirects", true);
        final HttpProtocol protocol = protocol(conf);
        try {
            fetch(protocol, "localhost", "/redirect");
        } finally {
            protocol.cleanup();
        }
        Assertions.assertEquals(EXPECTED, authorizationSeen.get("localhost/redirect"));
        Assertions.assertEquals(
                "null",
                authorizationSeen.get("127.0.0.1/target"),
                "the redirect is followed but its target is not a listed host");
    }

    @Test
    void sameOriginRedirectOnHostWhichIsNotListedDoesNotCarryTheCredentials() throws Exception {
        final Config conf = config("localhost");
        conf.put("http.allow.redirects", true);
        final HttpProtocol protocol = protocol(conf);
        try {
            fetch(protocol, "127.0.0.1", "/redirect-same-origin");
        } finally {
            protocol.cleanup();
        }
        Assertions.assertEquals("null", authorizationSeen.get("127.0.0.1/redirect-same-origin"));
        Assertions.assertEquals(
                "null",
                authorizationSeen.get("127.0.0.1/target"),
                "a redirect within the origin keeps the headers, which must not include them");
    }

    @Test
    void credentialsAreNotSentWithoutHosts() throws Exception {
        final HttpProtocol protocol = protocol(config(null));
        try {
            fetch(protocol, "localhost", "/page.html");
            fetch(protocol, "127.0.0.1", "/page.html");
        } finally {
            protocol.cleanup();
        }
        Assertions.assertEquals("null", authorizationSeen.get("localhost/page.html"));
        Assertions.assertEquals("null", authorizationSeen.get("127.0.0.1/page.html"));
    }

    @Test
    void entriesWhichAreNotPlainHostsAreIgnored() throws Exception {
        final HttpProtocol protocol =
                protocol(
                        config(
                                List.of(
                                        "http://localhost",
                                        "localhost:" + HTTP_PORT,
                                        // the default port, which HttpUrl drops when parsing
                                        "localhost:80",
                                        "localhost/page.html",
                                        "*.localhost")));
        try {
            fetch(protocol, "localhost", "/page.html");
        } finally {
            protocol.cleanup();
        }
        Assertions.assertEquals("null", authorizationSeen.get("localhost/page.html"));
    }
}
