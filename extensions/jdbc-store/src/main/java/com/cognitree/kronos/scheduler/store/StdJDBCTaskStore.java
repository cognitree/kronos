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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A standard JDBC based implementation of {@link TaskStore}.
 */
public class StdJDBCTaskStore implements TaskStore {
    private static final Logger logger = LoggerFactory.getLogger(StdJDBCTaskStore.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String INSERT_TASK = "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private static final String UPDATE_TASK = "UPDATE tasks SET status = ?, status_message = ?, submitted_at = ?, " +
            "completed_at = ?, context = ? WHERE name = ? AND job_id = ? AND namespace = ?";
    private static final String LOAD_ALL_TASKS_BY_NAMESPACE = "SELECT * FROM tasks WHERE namespace = ?";
    private static final String LOAD_TASK = "SELECT * FROM tasks WHERE name = ? AND job_id = ? AND namespace = ?";
    private static final String LOAD_TASK_BY_STATUS = "SELECT * FROM tasks WHERE status IN ($statuses)";
    private static final String LOAD_TASK_BY_JOB_ID = "SELECT * FROM tasks WHERE job_id = ? AND namespace = ?";
    private static final String DELETE_TASK = "DELETE FROM tasks WHERE name = ? AND job_id = ? AND namespace = ?";

    private static final TypeReference<Map<String, Object>> PROPERTIES_TYPE_REF =
            new TypeReference<Map<String, Object>>() {
            };
    private static final TypeReference<List<String>> DEPENDS_ON_TYPE_REF =
            new TypeReference<List<String>>() {
            };

    private BasicDataSource dataSource;

    @Override
    public void init(ObjectNode storeConfig) {
        logger.info("Initializing standard JDBC task store");
        initDataSource(storeConfig);
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

    @Override
    public void store(Task task) throws StoreException {
        logger.debug("Received request to store task {}", task);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_TASK)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, task.getName());
            preparedStatement.setString(++paramIndex, task.getJob());
            preparedStatement.setString(++paramIndex, task.getNamespace());
            preparedStatement.setString(++paramIndex, task.getType());
            preparedStatement.setString(++paramIndex, task.getTimeoutPolicy());
            preparedStatement.setString(++paramIndex, task.getMaxExecutionTime());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(task.getDependsOn()));
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(task.getProperties()));
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(task.getContext()));
            preparedStatement.setString(++paramIndex, task.getStatus().name());
            preparedStatement.setString(++paramIndex, task.getStatusMessage());
            JDBCUtil.setLong(preparedStatement, ++paramIndex, task.getCreatedAt());
            JDBCUtil.setLong(preparedStatement, ++paramIndex, task.getSubmittedAt());
            JDBCUtil.setLong(preparedStatement, ++paramIndex, task.getCompletedAt());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error storing task {} into database", task, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Task> loadByStatusIn(String namespace) throws StoreException {
        logger.debug("Received request to get all tasks in namespace {}", namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_ALL_TASKS_BY_NAMESPACE)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, namespace);
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<Task> tasks = new ArrayList<>();
            while (resultSet.next()) {
                tasks.add(getTask(resultSet));
            }
            return tasks;
        } catch (Exception e) {
            logger.error("Error loading all tasks from database in namespace {}", namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public Task load(TaskId taskId) throws StoreException {
        logger.debug("Received request to load task with id {}", taskId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_TASK)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, taskId.getName());
            preparedStatement.setString(++paramIndex, taskId.getJob());
            preparedStatement.setString(++paramIndex, taskId.getNamespace());
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return getTask(resultSet);
            }
        } catch (Exception e) {
            logger.error("Error loading task with id {} from database", taskId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
        return null;
    }

    @Override
    public List<Task> loadByJobId(String jobId, String namespace) throws StoreException {
        logger.debug("Received request to get all tasks with job id {}, namespace {}", jobId, namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_TASK_BY_JOB_ID)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, jobId);
            preparedStatement.setString(++paramIndex, namespace);
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<Task> tasks = new ArrayList<>();
            while (resultSet.next()) {
                tasks.add(getTask(resultSet));
            }
            return tasks;
        } catch (Exception e) {
            logger.error("Error fetching tasks with job id {}, namespace {} from database", jobId, namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Task> loadByStatus(List<Status> statuses, String namespace) throws StoreException {
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
            logger.error("Error fetching task with status in {} from database", statuses, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void update(Task task) throws StoreException {
        TaskId taskId = task.getIdentity();
        logger.debug("Received request to update task to {}", task);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_TASK)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, task.getStatus().name());
            preparedStatement.setString(++paramIndex, task.getStatusMessage());
            JDBCUtil.setLong(preparedStatement, ++paramIndex, task.getSubmittedAt());
            JDBCUtil.setLong(preparedStatement, ++paramIndex, task.getCompletedAt());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(task.getContext()));
            preparedStatement.setString(++paramIndex, taskId.getName());
            preparedStatement.setString(++paramIndex, taskId.getJob());
            preparedStatement.setString(++paramIndex, taskId.getNamespace());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error updating task with to {}", task, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void delete(TaskId taskId) throws StoreException {
        logger.debug("Received request to delete task with id {}", taskId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE_TASK)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, taskId.getName());
            preparedStatement.setString(++paramIndex, taskId.getJob());
            preparedStatement.setString(++paramIndex, taskId.getNamespace());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error("Error deleting task with id {} from database", taskId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    private Task getTask(ResultSet resultSet) throws Exception {
        int paramIndex = 0;
        MutableTask task = new MutableTask();
        task.setName(resultSet.getString(++paramIndex));
        task.setJob(resultSet.getString(++paramIndex));
        task.setNamespace(resultSet.getString(++paramIndex));
        task.setType(resultSet.getString(++paramIndex));
        task.setTimeoutPolicy(resultSet.getString(++paramIndex));
        task.setMaxExecutionTime(resultSet.getString(++paramIndex));
        task.setDependsOn(MAPPER.readValue(resultSet.getString(++paramIndex), DEPENDS_ON_TYPE_REF));
        task.setProperties(MAPPER.readValue(resultSet.getString(++paramIndex), PROPERTIES_TYPE_REF));
        task.setContext(MAPPER.readValue(resultSet.getString(++paramIndex), PROPERTIES_TYPE_REF));
        task.setStatus(Status.valueOf(resultSet.getString(++paramIndex)));
        task.setStatusMessage(resultSet.getString(++paramIndex));
        task.setCreatedAt(JDBCUtil.getLong(resultSet, ++paramIndex));
        task.setSubmittedAt(JDBCUtil.getLong(resultSet, ++paramIndex));
        task.setCompletedAt(JDBCUtil.getLong(resultSet, ++paramIndex));
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
