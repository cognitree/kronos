/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognitree.kronos.scheduler.store.jdbc;


import com.fasterxml.jackson.databind.node.ObjectNode;
import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.ServerAcl;
import org.quartz.impl.jdbcjobstore.HSQLDBDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.cognitree.kronos.scheduler.store.jdbc.JDBCUtil.HSQLDB_DRIVER_CLASS;

public class EmbeddedHSQLStoreService extends StdJDBCStoreService {
    private static final Logger logger = LoggerFactory.getLogger(HSQLDBDelegate.class);

    private static final String HSQL_HOST = "127.0.0.1";
    private static final String DB_PATH = "dbPath";
    private static final String HSQL_CONNECTION_URL_PREFIX = "jdbc:hsqldb:hsql://";
    private static final String KRONOS_SQL_SCRIPT = "kronos.sql";

    private final Server server = new Server();

    private String dbPath = "/tmp";

    public EmbeddedHSQLStoreService(ObjectNode config) {
        super(config);
    }

    @Override
    public void init() throws Exception {
        config.put(DRIVER_CLASS, HSQLDB_DRIVER_CLASS);
        if (config.hasNonNull(DB_PATH)) {
            dbPath = config.get(DB_PATH).asText();
        }
        String connectionUrl = HSQL_CONNECTION_URL_PREFIX + HSQL_HOST + "/" + dbPath + "/kronos";
        config.put(CONNECTION_URL, connectionUrl);
        config.put(SQL_SCRIPT, KRONOS_SQL_SCRIPT);
        super.init();
    }

    @Override
    public void start() throws Exception {
        startEmbeddedHSQLDB();
        super.start();
    }

    private void startEmbeddedHSQLDB() throws IOException, ServerAcl.AclFormatException {
        logger.info("Starting embedded HSQLDB running on " + HSQL_HOST);
        HsqlProperties hsqlProperties = new HsqlProperties();
        hsqlProperties.setProperty("server.database.0",
                "file:" + dbPath + "/kronos" + ";user=" + username + ";password=" + password);
        hsqlProperties.setProperty("server.dbname.0", "kronos");
        server.setProperties(hsqlProperties);
        server.start();
    }

    @Override
    public void stop() {
        super.stop();
        server.stop();
    }
}
