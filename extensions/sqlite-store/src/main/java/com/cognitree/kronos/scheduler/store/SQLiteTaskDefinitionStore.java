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

import com.cognitree.kronos.model.definitions.TaskDefinition;
import com.cognitree.kronos.model.definitions.TaskDefinitionId;
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
import java.util.Map;

/**
 * A SQLite implementation of {@link TaskStore}.
 */
public class SQLiteTaskDefinitionStore implements TaskDefinitionStore {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteTaskDefinitionStore.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String INSERT_REPLACE_TASK_DEFINITION_DEFINITION = "INSERT OR REPLACE INTO task_definitions VALUES (?,?,?)";
    private static final String LOAD_TASK_DEFINITION_BY_NAME = "SELECT * FROM task_definitions WHERE name = ?";
    private static final String LOAD_ALL_TASK_DEFINITION = "SELECT * FROM task_definitions";
    private static final String UPDATE_TASK_DEFINITION = "UPDATE task_definitions SET type = ?, properties = ? " +
            " WHERE name = ?";
    private static final String DELETE_TASK_DEFINITION_BY_NAME = "DELETE FROM task_definitions WHERE name = ?";
    private static final String DDL_CREATE_TASK_DEFINITION_SQL = "CREATE TABLE IF NOT EXISTS task_definitions (" +
            "name string," +
            "type string," +
            "properties string," +
            "PRIMARY KEY(name)" +
            ")";

    private static final TypeReference<Map<String, Object>> PROPERTIES_TYPE_REF =
            new TypeReference<Map<String, Object>>() {
            };
    private BasicDataSource dataSource;

    @Override
    public void init(ObjectNode storeConfig) throws Exception {
        logger.info("Initializing SQLite store for task definitions");
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
            statement.executeUpdate(DDL_CREATE_TASK_DEFINITION_SQL);
        }
    }

    @Override
    public void store(TaskDefinition taskDefinition) {
        logger.debug("Received request to store task definition {}", taskDefinition);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_REPLACE_TASK_DEFINITION_DEFINITION)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, taskDefinition.getName());
            preparedStatement.setString(++paramIndex, taskDefinition.getType());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(taskDefinition.getProperties()));
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error storing task definition {} into database", taskDefinition, e);
        }
    }

    @Override
    public List<TaskDefinition> load() {
        logger.debug("Received request to load all task definition");
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_ALL_TASK_DEFINITION)) {
            final ResultSet resultSet = preparedStatement.executeQuery();
            final ArrayList<TaskDefinition> taskDefinitions = new ArrayList<>();
            while (resultSet.next()) {
                taskDefinitions.add(getTaskDefinition(resultSet));
            }
            return taskDefinitions;
        } catch (Exception e) {
            logger.error("Error fetching all task definition from database", e);
        }
        return Collections.emptyList();
    }

    @Override
    public TaskDefinition load(TaskDefinitionId taskDefinitionId) {
        logger.debug("Received request to load task definition with id {}", taskDefinitionId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_TASK_DEFINITION_BY_NAME)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, taskDefinitionId.getName());
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return getTaskDefinition(resultSet);
            }
        } catch (Exception e) {
            logger.error("Error fetching task definition with id {} from database", taskDefinitionId, e);
        }
        return null;
    }

    @Override
    public void update(TaskDefinition taskDefinition) {
        TaskDefinitionId taskDefinitionId = taskDefinition.getIdentity();
        logger.debug("Received request to update task definition with id {} to {}", taskDefinitionId, taskDefinition);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_TASK_DEFINITION)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, taskDefinition.getType());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(taskDefinition.getProperties()));
            preparedStatement.setString(++paramIndex, taskDefinitionId.getName());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error updating task definition with id {} to {}", taskDefinitionId, taskDefinition, e);
        }
    }

    @Override
    public void delete(TaskDefinitionId taskDefinitionId) {
        logger.debug("Received request to delete task definition with id {}", taskDefinitionId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE_TASK_DEFINITION_BY_NAME)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, taskDefinitionId.getName());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error("Error deleting task definition with id {} from database", taskDefinitionId, e);
        }
    }

    private TaskDefinition getTaskDefinition(ResultSet resultSet) throws Exception {
        int paramIndex = 0;
        TaskDefinition task = new TaskDefinition();
        task.setName(resultSet.getString(++paramIndex));
        task.setType(resultSet.getString(++paramIndex));
        task.setProperties(MAPPER.readValue(resultSet.getString(++paramIndex), PROPERTIES_TYPE_REF));
        return task;
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
