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


import com.cognitree.kronos.scheduler.store.JobStore;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreProvider;
import com.cognitree.kronos.scheduler.store.TaskStore;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.ServerAcl;
import org.quartz.SchedulerException;
import org.quartz.impl.jdbcjobstore.HSQLDBDelegate;
import org.quartz.impl.jdbcjobstore.InvalidConfigurationException;
import org.quartz.impl.jdbcjobstore.JobStoreTX;
import org.quartz.utils.DBConnectionManager;
import org.quartz.utils.PoolingConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;

public class EmbeddedHSQLStoreProvider implements StoreProvider {
    private static final Logger logger = LoggerFactory.getLogger(HSQLDBDelegate.class);

    private static final String DRIVER_CLASS_NAME = "org.hsqldb.jdbcDriver";
    private static final String HSQLDB_DELEGATE = "org.quartz.impl.jdbcjobstore.HSQLDBDelegate";
    private static final String HSQL_HOST = "127.0.0.1";
    private static final String MIN_IDLE_CONNECTION = "minIdleConnection";
    private static final String MAX_IDLE_CONNECTION = "maxIdleConnection";
    private static final String MAX_OPEN_PREPARED_STATEMENTS = "maxOpenPreparedStatements";
    private static final String DB_PATH = "dbPath";
    private static final String HSQL_CONNECTION_URL_PREFIX = "jdbc:hsqldb:hsql://";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String DEFAULT_USERNAME = "SA";
    private static final String DEFAULT_PASSWORD = "";
    private static final String KRONOS_SQL_SCRIPT = "kronos.sql";
    private static final String DB_VALIDATION_QUERY = "select 0";

    private final Server server = new Server();

    private String dbPath = "/tmp";
    private String dbUrl;
    private String username = DEFAULT_USERNAME;
    private String password = DEFAULT_PASSWORD;
    private int minIdleConnection = 0;
    private int maxIdleConnection = 8;
    private int maxOpenPreparedStatements = -1;

    private NamespaceStore namespaceStore;
    private WorkflowStore workflowStore;
    private WorkflowTriggerStore workflowTriggerStore;
    private JobStore jobStore;
    private TaskStore taskStore;
    private org.quartz.spi.JobStore quartzJobStore;
    private BasicDataSource dataSource;

    @Override
    public void init(ObjectNode config) throws Exception {
        dbPath = config.get(DB_PATH).asText();
        dbUrl = HSQL_CONNECTION_URL_PREFIX + HSQL_HOST + "/" + dbPath + "/kronos";
        if (config.hasNonNull(USERNAME)) {
            username = config.get(USERNAME).asText();
            if (config.hasNonNull(PASSWORD)) {
                password = config.get(PASSWORD).asText();
            }
        }
        if (config.hasNonNull(MIN_IDLE_CONNECTION)) {
            minIdleConnection = config.get(MIN_IDLE_CONNECTION).asInt();
        }
        if (config.hasNonNull(MAX_IDLE_CONNECTION)) {
            maxIdleConnection = config.get(MAX_IDLE_CONNECTION).asInt();
        }
        if (config.hasNonNull(MAX_OPEN_PREPARED_STATEMENTS)) {
            maxOpenPreparedStatements = config.get(MAX_OPEN_PREPARED_STATEMENTS).asInt();
        }
        startEmbeddedHSQLDB();
        buildDataSource();
        createStore();
        initDatabase();
    }

    private void startEmbeddedHSQLDB() throws IOException, ServerAcl.AclFormatException {
        HsqlProperties hsqlProperties = new HsqlProperties();
        hsqlProperties.setProperty("server.database.0",
                "file:" + dbPath + "/kronos" + ";user=" + username + ";password=" + password);
        hsqlProperties.setProperty("server.dbname.0", "kronos");
        server.setProperties(hsqlProperties);
        server.start();
    }

    private void buildDataSource() {
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(DRIVER_CLASS_NAME);
        dataSource.setUrl(HSQL_CONNECTION_URL_PREFIX + HSQL_HOST + "/" + dbPath + "/kronos");
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMinIdle(minIdleConnection);
        dataSource.setMaxIdle(maxIdleConnection);
        dataSource.setMaxOpenPreparedStatements(maxOpenPreparedStatements);
    }

    private void createStore() throws SQLException, SchedulerException, InvalidConfigurationException {
        namespaceStore = new StdJDBCNamespaceStore(dataSource);
        workflowStore = new StdJDBCWorkflowStore(dataSource);
        workflowTriggerStore = new StdJDBCWorkflowTriggerStore(dataSource);
        jobStore = new StdJDBCJobStore(dataSource);
        taskStore = new StdJDBCTaskStore(dataSource);
        quartzJobStore = getQuartJobStore();
    }

    private JobStoreTX getQuartJobStore() throws SQLException, SchedulerException, InvalidConfigurationException {
        PoolingConnectionProvider connectionProvider = new PoolingConnectionProvider(DRIVER_CLASS_NAME, dbUrl,
                username, password, maxIdleConnection, DB_VALIDATION_QUERY);
        DBConnectionManager.getInstance().addConnectionProvider("kronos", connectionProvider);
        JobStoreTX jdbcJobStore = new JobStoreTX();
        jdbcJobStore.setDriverDelegateClass(HSQLDB_DELEGATE);
        jdbcJobStore.setDataSource("kronos");
        jdbcJobStore.setTablePrefix("QRTZ_");
        return jdbcJobStore;
    }

    private void initDatabase() throws SQLException {
        final InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(KRONOS_SQL_SCRIPT);
        ScriptRunner scriptRunner = new ScriptRunner(dataSource.getConnection());
        scriptRunner.runScript(new InputStreamReader(resourceAsStream));
    }

    @Override
    public NamespaceStore getNamespaceStore() {
        return namespaceStore;
    }

    @Override
    public WorkflowStore getWorkflowStore() {
        return workflowStore;
    }

    @Override
    public WorkflowTriggerStore getWorkflowTriggerStore() {
        return workflowTriggerStore;
    }

    @Override
    public JobStore getJobStore() {
        return jobStore;
    }

    @Override
    public TaskStore getTaskStore() {
        return taskStore;
    }

    @Override
    public org.quartz.spi.JobStore getQuartzJobStore() {
        return quartzJobStore;
    }

    @Override
    public void stop() {
        try {
            dataSource.close();
        } catch (SQLException e) {
            logger.error("Error closing data source", e);
        }
        server.stop();
    }
}
