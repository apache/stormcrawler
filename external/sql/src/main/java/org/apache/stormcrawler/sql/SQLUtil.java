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

package org.apache.stormcrawler.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class SQLUtil {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(SQLUtil.class);

    private SQLUtil() {}

    public static Connection getConnection(Map<String, Object> stormConf) throws SQLException {
        // SQL connection details
        Map<String, String> sqlConf = (Map<String, String>) stormConf.get("sql.connection");

        if (sqlConf == null) {
            throw new RuntimeException(
                    "Missing SQL connection config, add a section 'sql.connection' to the configuration");
        }

        String url = sqlConf.get("url");
        if (url == null) {
            throw new RuntimeException(
                    "Missing SQL url, add an entry 'url' to the section 'sql.connection' of the configuration");
        }

        Properties props = new Properties();

        props.putAll(sqlConf);

        return DriverManager.getConnection(url, props);
    }

    public static void closeResource(final AutoCloseable resource, final String resourceName) {
        if (resource != null) {
            try {
                resource.close();
            } catch (Exception e) {
                LOG.error("Error closing {}", resourceName, e);
            }
        }
    }
}
