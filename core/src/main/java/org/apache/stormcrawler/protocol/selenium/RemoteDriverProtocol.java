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
package org.apache.stormcrawler.protocol.selenium;

import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.storm.Config;
import org.apache.stormcrawler.protocol.Protocol;
import org.apache.stormcrawler.util.ConfUtils;
import org.openqa.selenium.WebDriver.Timeouts;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

/**
 * Delegates the requests to one or more remote selenium servers. The processes must be started /
 * stopped separately. The URLs to connect to are specified with the config 'selenium.addresses'.
 */
public class RemoteDriverProtocol extends SeleniumProtocol {

    private void substituteUserAgent(Map<String, Object> keyvals, final String userAgentString) {
        if (keyvals == null) return;

        for (Entry<String, Object> entry : keyvals.entrySet()) {
            Object val = entry.getValue();
            // substitute variable $useragent for the real value
            if (val instanceof String && val.toString().contains("$useragent")) {
                String newval = ((String) val).replaceAll("\\$useragent", userAgentString);
                entry.setValue(newval);
            } else if (val instanceof Map<?, ?>) {
                substituteUserAgent((Map<String, Object>) val, userAgentString);
            } else if (val instanceof List<?>) {
                List<String> newList = new ArrayList<>();
                ((List<String>) val)
                        .forEach(
                                s -> {
                                    String newval = s.replaceAll("\\$useragent", userAgentString);
                                    newList.add(newval);
                                });
                entry.setValue(newList);
            }
        }
    }

    @Override
    public void configure(Config conf) {
        super.configure(conf);

        final String userAgentString = getAgentString(conf);

        // custom capabilities
        final Map<String, Object> confCapabilities =
                (Map<String, Object>) conf.get("selenium.capabilities");

        substituteUserAgent(confCapabilities, userAgentString);

        // see https://github.com/SeleniumHQ/selenium/wiki/DesiredCapabilities
        final DesiredCapabilities capabilities = new DesiredCapabilities();

        confCapabilities.forEach((k, v) -> capabilities.setCapability(k, v));

        LOG.info("Configuring Selenium with {}", capabilities);

        // load addresses from config
        List<String> addresses = ConfUtils.loadListFromConf("selenium.addresses", conf);
        if (addresses.size() == 0) {
            throw new RuntimeException("No value found for selenium.addresses");
        }

        final boolean tracing = ConfUtils.getBoolean(conf, "selenium.tracing", false);

        // TEMPORARY: draw attention to config change
        for (String p : new String[] {"implicitlyWait", "pageLoadTimeout", "scriptTimeout"}) {
            if (conf.containsKey("selenium." + p)) {
                String message = "selenium." + p + " is deprecated. Please use selenium.timeouts!";
                LOG.error(message);
                throw new RuntimeException(message);
            }
        }

        for (String cdaddress : addresses) {
            try {
                RemoteWebDriver driver =
                        new RemoteWebDriver(new URL(cdaddress), capabilities, tracing);
                // setting timouts
                // see https://www.browserstack.com/guide/understanding-selenium-timeouts
                Timeouts touts = driver.manage().timeouts();
                Map<String, Number> timeouts = (Map<String, Number>) conf.get("selenium.timeouts");
                if (timeouts != null) {
                    long implicitTimeout = timeouts.getOrDefault("implicit", -1).longValue();
                    long pageLoadTimeout = timeouts.getOrDefault("pageLoad", -1).longValue();
                    long scriptTimeout = timeouts.getOrDefault("script", -1).longValue();
                    if (implicitTimeout != -1) {
                        touts.implicitlyWait(Duration.ofMillis(implicitTimeout));
                    }
                    if (pageLoadTimeout != -1) {
                        touts.pageLoadTimeout(Duration.ofMillis(pageLoadTimeout));
                    }
                    if (scriptTimeout != -1) {
                        touts.scriptTimeout(Duration.ofMillis(scriptTimeout));
                    }
                }
                drivers.add(driver);
            } catch (Exception e) {
                LOG.error(e.getLocalizedMessage(), e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Protocol.main(new RemoteDriverProtocol(), args);
    }
}
