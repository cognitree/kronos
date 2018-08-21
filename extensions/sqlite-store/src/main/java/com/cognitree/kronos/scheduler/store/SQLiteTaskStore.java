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

import com.cognitree.kronos.model.MutableTask;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskId;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Map;

/**
 * A SQLite implementation of {@link TaskStore}.
 */
public class SQLiteTaskStore implements TaskStore {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteTaskStore.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String INSERT_REPLACE_TASK = "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private static final String UPDATE_TASK = "UPDATE tasks SET status = ?, status_message = ?, submitted_at = ?, " +
            "completed_at = ?, context = ? WHERE id = ? AND workflow_id = ? AND namespace = ?";
    private static final String LOAD_ALL_TASKS = "SELECT * FROM tasks";
    private static final String LOAD_TASK = "SELECT * FROM tasks WHERE id = ? AND workflow_id = ? AND namespace = ?";
    private static final String LOAD_TASK_BY_STATUS = "SELECT * FROM tasks WHERE status IN ($statuses)";
    private static final String LOAD_TASK_BY_NAME_WORKFLOW_ID = "SELECT * FROM tasks WHERE name = ? AND workflow_id = ?" +
            " AND namespace = ?";
    private static final String LOAD_TASK_BY_WORKFLOW_ID = "SELECT * FROM tasks WHERE workflow_id = ? AND namespace = ?";
    private static final String DELETE_TASK = "DELETE FROM tasks WHERE id = ? AND workflow_id = ? AND namespace = ?";
    private static final String DDL_CREATE_TASK_SQL = "CREATE TABLE IF NOT EXISTS tasks (" +
            "id string," +
            "workflow_id string," +
            "name string," +
            "namespace string," +
            "type string," +
            "timeout_policy string," +
            "max_execution_time string," +
            "depends_on string," +
            "properties string," +
            "context string," +
            "status string," +
            "status_message string," +
            "created_at integer," +
            "submitted_at integer," +
            "completed_at integer," +
            "PRIMARY KEY(id, namespace)" +
            ")";
    private static final String CREATE_TASK_INDEX_NAME_WORKFLOW_SQL = "CREATE INDEX IF NOT EXISTS tasks_name_workflow_idx " +
            "on tasks (name, workflow_id, namespace)";
    private static final String CREATE_TASK_INDEX_ID_WORKFLOW_SQL = "CREATE INDEX IF NOT EXISTS tasks_id_workflow_idx " +
            "on tasks (id, workflow_id, namespace)";
    private static final String CREATE_TASK_INDEX_WORKFLOW_ID_SQL = "CREATE INDEX IF NOT EXISTS tasks_workflow_idx " +
            "on tasks (workflow_id, namespace)";
    private static final TypeReference<Map<String, Object>> PROPERTIES_TYPE_REF =
            new TypeReference<Map<String, Object>>() {
            };
    private static final TypeReference<List<String>> DEPENDS_ON_TYPE_REF =
            new TypeReference<List<String>>() {
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
            statement.executeUpdate(DDL_CREATE_TASK_SQL);
            statement.executeUpdate(CREATE_TASK_INDEX_NAME_WORKFLOW_SQL);
            statement.executeUpdate(CREATE_TASK_INDEX_ID_WORKFLOW_SQL);
            statement.executeUpdate(CREATE_TASK_INDEX_WORKFLOW_ID_SQL);
        }
    }

    @Override
    public void store(Task task) {
        logger.debug("Received request to store task {}", task);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_REPLACE_TASK)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, task.getId());
            preparedStatement.setString(++paramIndex, task.getWorkflowId());
            preparedStatement.setString(++paramIndex, task.getName());
            preparedStatement.setString(++paramIndex, task.getNamespace());
            preparedStatement.setString(++paramIndex, task.getType());
            preparedStatement.setString(++paramIndex, task.getTimeoutPolicy());
            preparedStatement.setString(++paramIndex, task.getMaxExecutionTime());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(task.getDependsOn()));
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(task.getProperties()));
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(task.getContext()));
            preparedStatement.setString(++paramIndex, task.getStatus().name());
            preparedStatement.setString(++paramIndex, task.getStatusMessage());
            preparedStatement.setLong(++paramIndex, task.getCreatedAt());
            preparedStatement.setLong(++paramIndex, task.getSubmittedAt());
            preparedStatement.setLong(++paramIndex, task.getCompletedAt());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error storing task {} into database", task, e);
        }
    }

    @Override
    public List<Task> load() {
        logger.debug("Received request to get all tasks");
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_ALL_TASKS)) {
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<Task> tasks = new ArrayList<>();
            while (resultSet.next()) {
                tasks.add(getTask(resultSet));
            }
            return tasks;
        } catch (Exception e) {
            logger.error("Error loading all tasks from database", e);
            return Collections.emptyList();
        }
    }

    @Override
    public Task load(TaskId taskId) {
        logger.debug("Received request to load task with id {}", taskId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_TASK)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, taskId.getId());
            preparedStatement.setString(++paramIndex, taskId.getWorkflowId());
            preparedStatement.setString(++paramIndex, taskId.getNamespace());
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return getTask(resultSet);
            }
        } catch (Exception e) {
            logger.error("Error loading task with id {} from database", taskId, e);
        }
        return null;
    }

    @Override
    public void update(Task task) {
        TaskId taskId = task.getIdentity();
        logger.debug("Received request to update task with id {} to {}", taskId, task);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_TASK)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, task.getStatus().name());
            preparedStatement.setString(++paramIndex, task.getStatusMessage());
            preparedStatement.setLong(++paramIndex, task.getSubmittedAt());
            preparedStatement.setLong(++paramIndex, task.getCompletedAt());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(task.getContext()));
            preparedStatement.setString(++paramIndex, taskId.getId());
            preparedStatement.setString(++paramIndex, taskId.getWorkflowId());
            preparedStatement.setString(++paramIndex, taskId.getNamespace());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error updating task with id {} to {}", taskId, task, e);
        }
    }

    @Override
    public List<Task> loadByWorkflowId(String workflowId, String namespace) {
        logger.debug("Received request to get all tasks with workflow id {}, namespace {}", workflowId, namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_TASK_BY_WORKFLOW_ID)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowId);
            preparedStatement.setString(++paramIndex, namespace);
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<Task> tasks = new ArrayList<>();
            while (resultSet.next()) {
                tasks.add(getTask(resultSet));
            }
            return tasks;
        } catch (Exception e) {
            logger.error("Error fetching tasks with workflow id {}, namespace {} from database",
                    workflowId, namespace, e);
            return Collections.emptyList();
        }
    }

    @Override
    public List<Task> load(List<Status> statuses, String namespace) {
        // TODO handle namespace
        logger.debug("Received request to get all tasks with status in {}, namespace {}", statuses, namespace);
        String placeHolders = String.join(",", Collections.nCopies(statuses.size(), "?"));
        final String sqlQuery = LOAD_TASK_BY_STATUS.replace("$statuses", placeHolders);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
            for (int i = 0; i < statuses.size(); i++) {
                preparedStatement.setString(i + 1, statuses.get(i).name());
            }
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<Task> tasks = new ArrayList<>();
            while (resultSet.next()) {
                tasks.add(getTask(resultSet));
            }
            return tasks;
        } catch (Exception e) {
            logger.error("Error loading task with status in {} from database", statuses, e);
            return Collections.emptyList();
        }
    }

    @Override
    public List<Task> loadByNameAndWorkflowId(String taskName, String workflowId, String namespace) {
        logger.debug("Received request to get all tasks with name {}, workflow id {}, namespace {}",
                taskName, workflowId, namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_TASK_BY_NAME_WORKFLOW_ID)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, taskName);
            preparedStatement.setString(++paramIndex, workflowId);
            preparedStatement.setString(++paramIndex, namespace);
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<Task> tasks = new ArrayList<>();
            while (resultSet.next()) {
                tasks.add(getTask(resultSet));
            }
            return tasks;
        } catch (Exception e) {
            logger.error("Error loading task with name {}, workflow id {} from database", taskName, workflowId, e);
            return Collections.emptyList();
        }
    }

    @Override
    public void delete(TaskId identity) {
        logger.debug("Received request to delete task with id {}", identity);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE_TASK)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, identity.getId());
            preparedStatement.setString(++paramIndex, identity.getWorkflowId());
            preparedStatement.setString(++paramIndex, identity.getNamespace());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error("Error deleting task with id {} from database", identity, e);
        }
    }

    private Task getTask(ResultSet resultSet) throws Exception {
        int paramIndex = 0;
        MutableTask task = new MutableTask();
        task.setId(resultSet.getString(++paramIndex));
        task.setWorkflowId(resultSet.getString(++paramIndex));
        task.setName(resultSet.getString(++paramIndex));
        task.setNamespace(resultSet.getString(++paramIndex));
        task.setType(resultSet.getString(++paramIndex));
        task.setTimeoutPolicy(resultSet.getString(++paramIndex));
        task.setMaxExecutionTime(resultSet.getString(++paramIndex));
        task.setDependsOn(MAPPER.readValue(resultSet.getString(++paramIndex), DEPENDS_ON_TYPE_REF));
        task.setProperties(MAPPER.readValue(resultSet.getString(++paramIndex), PROPERTIES_TYPE_REF));
        task.setContext(MAPPER.readValue(resultSet.getString(++paramIndex), PROPERTIES_TYPE_REF));
        task.setStatus(Status.valueOf(resultSet.getString(++paramIndex)));
        task.setStatusMessage(resultSet.getString(++paramIndex));
        task.setCreatedAt(resultSet.getLong(++paramIndex));
        task.setSubmittedAt(resultSet.getLong(++paramIndex));
        task.setCompletedAt(resultSet.getLong(++paramIndex));
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
