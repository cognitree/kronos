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

import com.cognitree.kronos.model.definitions.WorkflowDefinition;
import com.cognitree.kronos.model.definitions.WorkflowDefinition.WorkflowTask;
import com.cognitree.kronos.model.definitions.WorkflowDefinitionId;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A SQLite implementation of {@link TaskStore}.
 */
public class SQLiteWorkflowDefinitionStore implements WorkflowDefinitionStore {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteWorkflowDefinitionStore.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String INSERT_REPLACE_WORKFLOW_DEFINITION_DEFINITION = "INSERT OR REPLACE INTO workflow_definitions VALUES (?,?,?,?,?,?)";
    private static final String LOAD_ALL_WORKFLOW_DEFINITION = "SELECT * FROM workflow_definitions";
    private static final String UPDATE_WORKFLOW_DEFINITION = "UPDATE workflow_definitions set description = ?, schedule = ?," +
            " tasks = ?, enabled = ? where name = ? AND namespace = ?";
    private static final String DELETE_WORKFLOW_DEFINITION = "DELETE FROM workflow_definitions where name = ? AND namespace = ?";
    private static final String LOAD_WORKFLOW = "SELECT * FROM workflow_definitions where name = ? AND namespace = ?";
    private static final String DDL_CREATE_WORKFLOW_DEFINITION_SQL = "CREATE TABLE IF NOT EXISTS workflow_definitions (" +
            "name string," +
            "namespace string," +
            "description string," +
            "schedule string," +
            "tasks string," +
            "enabled boolean," +
            "PRIMARY KEY(name, namespace)" +
            ")";
    private static final TypeReference<List<WorkflowTask>> WORKFLOW_TASK_LIST_TYPE_REF =
            new TypeReference<List<WorkflowTask>>() {
            };

    private BasicDataSource dataSource;

    @Override
    public void init(ObjectNode storeConfig) throws Exception {
        logger.info("Initializing SQLite task store");
        initDataSource(storeConfig);
        initTaskStore();
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

    private void initTaskStore() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(30);
            statement.executeUpdate(DDL_CREATE_WORKFLOW_DEFINITION_SQL);
        }
    }

    @Override
    public void store(WorkflowDefinition workflowDefinition) {
        logger.debug("Received request to store workflow definition {}", workflowDefinition);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_REPLACE_WORKFLOW_DEFINITION_DEFINITION)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowDefinition.getName());
            preparedStatement.setString(++paramIndex, workflowDefinition.getNamespace());
            preparedStatement.setString(++paramIndex, workflowDefinition.getDescription());
            preparedStatement.setString(++paramIndex, workflowDefinition.getSchedule());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(workflowDefinition.getTasks()));
            preparedStatement.setBoolean(++paramIndex, workflowDefinition.isEnabled());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error storing workflow definition {} into database", workflowDefinition, e);
        }
    }

    @Override
    public List<WorkflowDefinition> load() {
        logger.debug("Received request to get all workflow");
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_ALL_WORKFLOW_DEFINITION)) {
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<WorkflowDefinition> workflowDefinitions = new ArrayList<>();
            while (resultSet.next()) {
                workflowDefinitions.add(getWorkflowDefinition(resultSet));
            }
            return workflowDefinitions;
        } catch (Exception e) {
            logger.error("Error fetching workflow definitions from database", e);
            return Collections.emptyList();
        }
    }

    @Override
    public WorkflowDefinition load(WorkflowDefinitionId workflowDefinitionId) {
        logger.debug("Received request to load workflow definition with id {}", workflowDefinitionId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_WORKFLOW)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowDefinitionId.getName());
            preparedStatement.setString(++paramIndex, workflowDefinitionId.getNamespace());
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return getWorkflowDefinition(resultSet);
            }
        } catch (Exception e) {
            logger.error("Error loading workflow definitions with id {} from database", workflowDefinitionId, e);
        }
        return null;
    }

    @Override
    public void update(WorkflowDefinition workflowDefinition) {
        logger.debug("Received request to update workflow definition {}", workflowDefinition);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_WORKFLOW_DEFINITION)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowDefinition.getDescription());
            preparedStatement.setString(++paramIndex, workflowDefinition.getSchedule());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(workflowDefinition.getTasks()));
            preparedStatement.setBoolean(++paramIndex, workflowDefinition.isEnabled());
            preparedStatement.setString(++paramIndex, workflowDefinition.getName());
            preparedStatement.setString(++paramIndex, workflowDefinition.getNamespace());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error updating workflow definition {} into database", workflowDefinition, e);
        }
    }

    @Override
    public void delete(WorkflowDefinitionId workflowDefinitionId) {
        logger.debug("Received request to delete workflow with id {}", workflowDefinitionId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE_WORKFLOW_DEFINITION)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowDefinitionId.getName());
            preparedStatement.setString(++paramIndex, workflowDefinitionId.getNamespace());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error("Error delete workflow definitions with id {} from database", workflowDefinitionId, e);
        }
    }

    private WorkflowDefinition getWorkflowDefinition(ResultSet resultSet) throws Exception {
        int paramIndex = 0;
        WorkflowDefinition workflowDefinition = new WorkflowDefinition();
        workflowDefinition.setName(resultSet.getString(++paramIndex));
        workflowDefinition.setNamespace(resultSet.getString(++paramIndex));
        workflowDefinition.setDescription(resultSet.getString(++paramIndex));
        workflowDefinition.setSchedule(resultSet.getString(++paramIndex));
        workflowDefinition.setTasks(MAPPER.readValue(resultSet.getString(++paramIndex), WORKFLOW_TASK_LIST_TYPE_REF));
        workflowDefinition.setEnabled(resultSet.getBoolean(++paramIndex));
        return workflowDefinition;
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
