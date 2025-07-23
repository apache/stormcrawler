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
import java.util.Map.Entry;
import java.util.Properties;

public class SQLUtil {

    private SQLUtil() {}

    public static Connection getConnection(Map<String, Object> stormConf) throws SQLException {
        // SQL connection details
        Map<String, Object> sqlConf = (Map<String, Object>) stormConf.get("sql.connection");

        if (sqlConf == null) {
            throw new RuntimeException(
                    "Missing SQL connection config, add a section 'sql.connection' to the configuration");
        }

        String url = (String) sqlConf.get("url");
        if (url == null) {
            throw new RuntimeException(
                    "Missing SQL url, add an entry 'url' to the section 'sql.connection' of the configuration");
        }

        Properties props = new Properties();

        for (Entry<String, Object> entry : sqlConf.entrySet()) {
            props.setProperty(entry.getKey(), (String) entry.getValue());
        }

        return DriverManager.getConnection(url, props);
    }
}
