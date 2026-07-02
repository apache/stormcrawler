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

import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/** Serve the content of the resources if any found * */
public class LocalResourceHandler extends AbstractHandler {

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

        // does the URL have a file element?
        if (s.length() < 2) {
            return;
        }

        baseRequest.setHandled(true);

        final String file = s.substring(1);
        InputStream stream = getClass().getClassLoader().getResourceAsStream(file);

        // not found?
        if (stream == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(stream, baos);
        byte[] barray = baos.toByteArray();

        response.setHeader("key", "value");
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("text/html");
        try (OutputStream out = response.getOutputStream()) {
            out.write(barray);
        }
    }
}
