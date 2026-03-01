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

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;

/** Takes care of initialising Jetty for testing protocol implementation * */
public abstract class AbstractProtocolTest {

    protected static Server httpServer;

    protected static final Integer HTTP_PORT = findRandomOpenPortOnAllLocalInterfaces();

    @BeforeEach
    void initJetty() throws Exception {
        if (httpServer != null) {
            return;
        }
        assertNotEquals(Integer.valueOf(-1), HTTP_PORT);
        httpServer = new Server(HTTP_PORT);
        final HandlerList handlers = new HandlerList();
        handlers.setHandlers(getHandlers());
        httpServer.setHandler(handlers);
        httpServer.start();
    }

    protected Handler[] getHandlers() {
        return new Handler[] {new WildcardResourceHandler()};
    }

    @AfterAll
    static void stopJetty() {
        try {
            httpServer.stop();
        } catch (Exception ignored) {
        }
    }

    private static Integer findRandomOpenPortOnAllLocalInterfaces() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            return -1;
        }
    }

    public static class WildcardResourceHandler extends AbstractHandler {

        @Override
        public void handle(
                String s,
                Request baseRequest,
                jakarta.servlet.http.HttpServletRequest httpServletRequest,
                jakarta.servlet.http.HttpServletResponse response)
                throws IOException {
            if (response.isCommitted() || baseRequest.isHandled()) {
                return;
            }
            baseRequest.setHandled(true);
            final String content = "Success!";
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("text/html");
            response.setContentLength(content.length());
            try (OutputStream out = response.getOutputStream()) {
                out.write(content.getBytes(StandardCharsets.UTF_8));
            }
        }
    }
}
