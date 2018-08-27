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

package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.model.definitions.WorkflowTrigger;
import com.cognitree.kronos.model.definitions.WorkflowTriggerId;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A SQLite implementation of {@link WorkflowTriggerStore}.
 */
public class SQLiteWorkflowTriggerStore implements WorkflowTriggerStore {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteWorkflowTriggerStore.class);

    private static final String INSERT_WORKFLOW_TRIGGER = "INSERT INTO workflow_triggers VALUES (?,?,?,?,?,?,?)";
    private static final String LOAD_ALL_WORKFLOW_TRIGGER_BY_NAMESPACE = "SELECT * FROM workflow_triggers " +
            "WHERE namespace = ?";
    private static final String LOAD_ALL_WORKFLOW_TRIGGER_BY_WORKFLOW_NAME = "SELECT * FROM workflow_triggers " +
            "WHERE workflow_name = ? AND namespace = ?";
    private static final String UPDATE_WORKFLOW_TRIGGER = "UPDATE workflow_triggers set description = ?, schedule = ?," +
            " tasks = ?, enabled = ? where name = ? AND workflow_name = ? AND namespace = ?";
    private static final String DELETE_WORKFLOW_TRIGGER = "DELETE FROM workflow_triggers where name = ? " +
            "AND workflow_name = ? AND namespace = ?";
    private static final String LOAD_WORKFLOW_TRIGGER = "SELECT * FROM workflow_triggers where name = ? " +
            "AND workflow_name = ? AND namespace = ?";
    private static final String DDL_CREATE_WORKFLOW_TRIGGER_SQL = "CREATE TABLE IF NOT EXISTS workflow_triggers (" +
            "name string," +
            "workflow_name string," +
            "namespace string," +
            "startAt string," +
            "schedule string," +
            "endAt string," +
            "enabled boolean," +
            "PRIMARY KEY(name, workflow_name, namespace)" +
            ")";

    private BasicDataSource dataSource;

    @Override
    public void init(ObjectNode storeConfig) throws Exception {
        logger.info("Initializing SQLite workflow trigger store");
        initDataSource(storeConfig);
        initWorkflowTriggerStore();
    }

    private void initDataSource(ObjectNode storeConfig) {
        dataSource = new BasicDataSource();
        dataSource.setUrl(storeConfig.get("connectionURL").asText());
        if (storeConfig.hasNonNull("username")) {
            dataSource.setUsername(storeConfig.get("username").asText());
            if (storeConfig.hasNonNull("password")) {
                dataSource.setPassword(storeConfig.get("password").asText());
            }
        }
        if (storeConfig.hasNonNull("minIdleConnection")) {
            dataSource.setMinIdle(storeConfig.get("minIdleConnection").asInt());
        }
        if (storeConfig.hasNonNull("maxIdleConnection")) {
            dataSource.setMaxIdle(storeConfig.get("maxIdleConnection").asInt());
        }
        if (storeConfig.hasNonNull("maxOpenPreparedStatements")) {
            dataSource.setMaxOpenPreparedStatements(storeConfig.get("maxOpenPreparedStatements").asInt());
        }
    }

    private void initWorkflowTriggerStore() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(30);
            statement.executeUpdate(DDL_CREATE_WORKFLOW_TRIGGER_SQL);
        }
    }

    @Override
    public void store(WorkflowTrigger workflowTrigger) {
        logger.debug("Received request to store workflow trigger {}", workflowTrigger);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_WORKFLOW_TRIGGER)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowTrigger.getName());
            preparedStatement.setString(++paramIndex, workflowTrigger.getWorkflowName());
            preparedStatement.setString(++paramIndex, workflowTrigger.getNamespace());
            preparedStatement.setLong(++paramIndex, workflowTrigger.getStartAt());
            preparedStatement.setString(++paramIndex, workflowTrigger.getSchedule());
            preparedStatement.setLong(++paramIndex, workflowTrigger.getEndAt());
            preparedStatement.setBoolean(++paramIndex, workflowTrigger.isEnabled());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error storing workflow trigger {} into database", workflowTrigger, e);
        }
    }

    @Override
    public List<WorkflowTrigger> load(String namespace) {
        logger.debug("Received request to get all workflow triggers in namespace {}", namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_ALL_WORKFLOW_TRIGGER_BY_NAMESPACE)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, namespace);
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<WorkflowTrigger> workflowTriggers = new ArrayList<>();
            while (resultSet.next()) {
                workflowTriggers.add(getWorkflowTrigger(resultSet));
            }
            return workflowTriggers;
        } catch (Exception e) {
            logger.error("Error fetching workflow triggers from database in namespace {}", namespace, e);
            return Collections.emptyList();
        }
    }

    @Override
    public List<WorkflowTrigger> loadByWorkflowName(String workflowName, String namespace) {
        logger.debug("Received request to get all workflow triggers with workflow name {} in namespace {}",
                workflowName, namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_ALL_WORKFLOW_TRIGGER_BY_WORKFLOW_NAME)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowName);
            preparedStatement.setString(++paramIndex, namespace);
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<WorkflowTrigger> workflowTriggers = new ArrayList<>();
            while (resultSet.next()) {
                workflowTriggers.add(getWorkflowTrigger(resultSet));
            }
            return workflowTriggers;
        } catch (Exception e) {
            logger.error("Error fetching workflow triggers from database with workflow name {} in namespace {}",
                    workflowName, namespace, e);
            return Collections.emptyList();
        }
    }

    @Override
    public WorkflowTrigger load(WorkflowTriggerId triggerId) {
        logger.debug("Received request to load workflow trigger with id {}", triggerId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_WORKFLOW_TRIGGER)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, triggerId.getName());
            preparedStatement.setString(++paramIndex, triggerId.getWorkflowName());
            preparedStatement.setString(++paramIndex, triggerId.getNamespace());
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return getWorkflowTrigger(resultSet);
            }
        } catch (Exception e) {
            logger.error("Error fetching workflow trigger with id {} from database", triggerId, e);
        }
        return null;
    }

    @Override
    public void update(WorkflowTrigger workflowTrigger) {
        logger.debug("Received request to update workflow trigger {}", workflowTrigger);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_WORKFLOW_TRIGGER)) {
            int paramIndex = 0;
            preparedStatement.setLong(++paramIndex, workflowTrigger.getStartAt());
            preparedStatement.setString(++paramIndex, workflowTrigger.getSchedule());
            preparedStatement.setLong(++paramIndex, workflowTrigger.getEndAt());
            preparedStatement.setBoolean(++paramIndex, workflowTrigger.isEnabled());
            preparedStatement.setString(++paramIndex, workflowTrigger.getName());
            preparedStatement.setString(++paramIndex, workflowTrigger.getWorkflowName());
            preparedStatement.setString(++paramIndex, workflowTrigger.getNamespace());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error updating workflow trigger {} into database", workflowTrigger, e);
        }
    }

    @Override
    public void delete(WorkflowTriggerId triggerId) {
        logger.debug("Received request to delete workflow trigger with id {}", triggerId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE_WORKFLOW_TRIGGER)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, triggerId.getName());
            preparedStatement.setString(++paramIndex, triggerId.getWorkflowName());
            preparedStatement.setString(++paramIndex, triggerId.getNamespace());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error("Error delete workflow trigger with id {} from database", triggerId, e);
        }
    }

    private WorkflowTrigger getWorkflowTrigger(ResultSet resultSet) throws Exception {
        int paramIndex = 0;
        WorkflowTrigger workflowTrigger = new WorkflowTrigger();
        workflowTrigger.setName(resultSet.getString(++paramIndex));
        workflowTrigger.setWorkflowName(resultSet.getString(++paramIndex));
        workflowTrigger.setNamespace(resultSet.getString(++paramIndex));
        workflowTrigger.setStartAt(resultSet.getLong(++paramIndex));
        workflowTrigger.setSchedule(resultSet.getString(++paramIndex));
        workflowTrigger.setEndAt(resultSet.getLong(++paramIndex));
        workflowTrigger.setEnabled(resultSet.getBoolean(++paramIndex));
        return workflowTrigger;
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
