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
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Abstract base class for SQL module tests that provides a shared MySQL container. Uses the
 * Testcontainers singleton pattern to ensure the container is created once and reused across all
 * test classes, improving test performance.
 */
@Testcontainers(disabledWithoutDocker = true)
@Timeout(value = 120, unit = TimeUnit.SECONDS)
abstract class AbstractSQLTest {

    private static final DockerImageName MYSQL_IMAGE = DockerImageName.parse("mysql:8.4.0");

    @Container
    private static final MySQLContainer<?> MYSQL_CONTAINER =
            new MySQLContainer<>(MYSQL_IMAGE)
                    .withDatabaseName("crawl")
                    .withUsername("crawler")
                    .withPassword("crawler")
                    .withReuse(true);

    static Connection testConnection;

    static Map<String, String> createSqlConnectionConfig() {
        Map<String, String> sqlConnection = new HashMap<>();
        sqlConnection.put("url", MYSQL_CONTAINER.getJdbcUrl());
        sqlConnection.put("user", MYSQL_CONTAINER.getUsername());
        sqlConnection.put("password", MYSQL_CONTAINER.getPassword());
        return sqlConnection;
    }

    void execute(String sql) throws SQLException {
        try (Statement stmt = testConnection.createStatement()) {
            stmt.execute(sql);
        }
    }

    @BeforeAll
    static void init() throws SQLException {
        testConnection =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword());
    }

    @BeforeEach
    void baseSetup() throws Exception {
        setupTestTables();
    }

    protected abstract void setupTestTables() throws Exception;

    @AfterAll
    static void cleanup() throws Exception {
        if (testConnection != null) {
            testConnection.close();
        }
    }
}
