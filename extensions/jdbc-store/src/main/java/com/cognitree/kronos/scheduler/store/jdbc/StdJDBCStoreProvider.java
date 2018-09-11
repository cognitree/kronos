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
import org.apache.ibatis.jdbc.RuntimeSqlException;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.quartz.SchedulerException;
import org.quartz.impl.jdbcjobstore.HSQLDBDelegate;
import org.quartz.impl.jdbcjobstore.InvalidConfigurationException;
import org.quartz.impl.jdbcjobstore.JobStoreTX;
import org.quartz.utils.DBConnectionManager;
import org.quartz.utils.PoolingConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;

public class StdJDBCStoreProvider implements StoreProvider {
    private static final Logger logger = LoggerFactory.getLogger(HSQLDBDelegate.class);

    protected static final String CONNECTION_URL = "connectionUrl";
    protected static final String DRIVER_CLASS = "driverClass";
    protected static final String QUARTZ_DRIVER_DELEGATE = "quartzDriverDelegate";
    protected static final String SQL_SCRIPT = "sqlScript";
    protected static final String MIN_IDLE_CONNECTION = "minIdleConnection";
    protected static final String MAX_IDLE_CONNECTION = "maxIdleConnection";
    protected static final String MAX_OPEN_PREPARED_STATEMENTS = "maxOpenPreparedStatements";
    protected static final String USERNAME = "username";
    protected static final String PASSWORD = "password";
    protected static final String DEFAULT_USERNAME = "SA";
    protected static final String DEFAULT_PASSWORD = "";
    protected static final String DB_VALIDATION_QUERY = "select 0";

    protected String connectionUrl;
    protected String driverClass;
    protected String quartzJDBCDelegate;
    protected String username = DEFAULT_USERNAME;
    protected String password = DEFAULT_PASSWORD;
    protected String sqlScript;
    protected int minIdleConnection = 0;
    protected int maxIdleConnection = 8;
    protected int maxOpenPreparedStatements = -1;

    private NamespaceStore namespaceStore;
    private WorkflowStore workflowStore;
    private WorkflowTriggerStore workflowTriggerStore;
    private JobStore jobStore;
    private TaskStore taskStore;
    private org.quartz.spi.JobStore quartzJobStore;
    private BasicDataSource dataSource;

    @Override
    public void init(ObjectNode config) throws Exception {
        parseConfig(config);
        buildDataSource();
        createStore();
        initDatabase();
    }

    private void parseConfig(ObjectNode config) throws Exception {
        if (!config.hasNonNull(CONNECTION_URL) || !config.hasNonNull(DRIVER_CLASS)) {
            throw new IllegalArgumentException("missing mandatory param: connectionUrl/ driverClass");
        }
        connectionUrl = config.get(CONNECTION_URL).asText();
        driverClass = config.get(DRIVER_CLASS).asText();
        if (config.hasNonNull(QUARTZ_DRIVER_DELEGATE)) {
            quartzJDBCDelegate = config.get(QUARTZ_DRIVER_DELEGATE).asText();
        } else {
            quartzJDBCDelegate = JDBCUtil.getQuartzJDBCDelegate(driverClass);
        }
        if (config.hasNonNull(SQL_SCRIPT)) {
            sqlScript = config.get(SQL_SCRIPT).asText();
        }

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
    }

    private void buildDataSource() {
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(driverClass);
        dataSource.setUrl(connectionUrl);
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
        PoolingConnectionProvider connectionProvider = new PoolingConnectionProvider(driverClass, connectionUrl,
                username, password, maxIdleConnection, DB_VALIDATION_QUERY);
        DBConnectionManager.getInstance().addConnectionProvider("kronos", connectionProvider);
        JobStoreTX jdbcJobStore = new JobStoreTX();
        jdbcJobStore.setDriverDelegateClass(quartzJDBCDelegate);
        jdbcJobStore.setDataSource("kronos");
        jdbcJobStore.setTablePrefix("QRTZ_");
        return jdbcJobStore;
    }

    private void initDatabase() throws SQLException, RuntimeSqlException {
        if (sqlScript != null) {
            final InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(sqlScript);
            ScriptRunner scriptRunner = new ScriptRunner(dataSource.getConnection());
            scriptRunner.runScript(new InputStreamReader(resourceAsStream));
        }
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
    }
}
