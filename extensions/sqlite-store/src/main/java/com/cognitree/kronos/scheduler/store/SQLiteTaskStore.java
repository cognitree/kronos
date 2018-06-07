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

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskDependencyInfo;
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
public class SQLiteTaskStore implements TaskStore {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteTaskStore.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String INSERT_REPLACE_TASK = "INSERT OR REPLACE INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
    private static final String UPDATE_TASK = "UPDATE tasks SET status = ?, status_message = ?, submitted_at = ?, " +
            "completed_at = ? WHERE id = ?";
    private static final String LOAD_TASK = "SELECT * FROM tasks WHERE id = ? AND task_group = ?";
    private static final String LOAD_TASK_BY_STATUS = "SELECT * FROM tasks WHERE status IN ($statuses)";
    private static final String LOAD_TASK_BY_NAME_GROUP = "SELECT * FROM tasks WHERE name = ? AND task_group = ? AND " +
            "created_at < ? AND created_at > ?";
    private static final String DDL_CREATE_TASK_SQL = "CREATE TABLE IF NOT EXISTS tasks (" +
            "id string," +
            "name string," +
            "task_group string," +
            "type string," +
            "timeout_policy string," +
            "depends_on string," +
            "properties string," +
            "status string," +
            "status_message string," +
            "created_at integer," +
            "submitted_at integer," +
            "completed_at integer," +
            "PRIMARY KEY(id)" +
            ")";
    private static final String CREATE_TASK_INDEX_SQL = "CREATE INDEX IF NOT EXISTS tasks_name_group_idx on tasks (name, task_group)";
    private static final TypeReference<Map<String, Object>> PROPERTIES_TYPE_REF =
            new TypeReference<Map<String, Object>>() {
            };
    private static final TypeReference<List<TaskDependencyInfo>> DEPENDENCY_INFO_LIST_TYPE_REF =
            new TypeReference<List<TaskDependencyInfo>>() {
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
            statement.executeUpdate(CREATE_TASK_INDEX_SQL);
        }
    }

    @Override
    public void store(Task task) {
        logger.debug("Received request to store task {}", task);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_REPLACE_TASK)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, task.getId());
            preparedStatement.setString(++paramIndex, task.getName());
            preparedStatement.setString(++paramIndex, task.getGroup());
            preparedStatement.setString(++paramIndex, task.getType());
            preparedStatement.setString(++paramIndex, task.getTimeoutPolicy());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(task.getDependsOn()));
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(task.getProperties()));
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
    public void update(String taskId, String taskGroup, Status status, String statusMessage,
                       long submittedAt, long completedAt) {
        logger.debug("Received request to update task properties id {}, group {}, status {}, status message {}, " +
                        "submitted at {}, completed at {}",
                taskId, taskGroup, status, statusMessage, submittedAt, completedAt);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_TASK)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, status.name());
            preparedStatement.setString(++paramIndex, statusMessage);
            preparedStatement.setLong(++paramIndex, submittedAt);
            preparedStatement.setLong(++paramIndex, completedAt);
            preparedStatement.setString(++paramIndex, taskId);
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("error updating task properties id {}, group {}, status {}, status message {}, " +
                            "submitted at {}, completed at {}",
                    taskId, taskGroup, status, statusMessage, submittedAt, completedAt);
        }
    }

    @Override
    public Task load(String taskId, String taskGroup) {
        logger.debug("Received request to get task with id {}, group {}", taskId, taskGroup);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_TASK)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, taskId);
            preparedStatement.setString(++paramIndex, taskGroup);
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return getTask(resultSet);
            }
        } catch (Exception e) {
            logger.error("Error fetching task with id {}, group {} from database", taskId, taskGroup, e);
        }
        return null;
    }

    @Override
    public List<Task> load(List<Status> statuses) {
        logger.debug("Received request to get all tasks with status in {}", statuses);
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
            return Collections.emptyList();
        }
    }

    @Override
    public List<Task> load(String taskName, String taskGroup, long createdBefore, long createdAfter) {
        logger.debug("Received request to get all tasks with name {}, group {}, created before {}, " +
                "created after {}", taskName, taskGroup, createdBefore, createdAfter);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_TASK_BY_NAME_GROUP)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, taskName);
            preparedStatement.setString(++paramIndex, taskGroup);
            preparedStatement.setLong(++paramIndex, createdBefore);
            preparedStatement.setLong(++paramIndex, createdAfter);
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<Task> tasks = new ArrayList<>();
            while (resultSet.next()) {
                tasks.add(getTask(resultSet));
            }
            return tasks;
        } catch (Exception e) {
            logger.error("Error fetching task with name {}, group {}, created before {}, created after {} " +
                    "from database", taskName, taskGroup, createdBefore, createdAfter, e);
            return Collections.emptyList();
        }
    }

    private Task getTask(ResultSet resultSet) throws Exception {
        int paramIndex = 0;
        Task task = new Task();
        task.setId(resultSet.getString(++paramIndex));
        task.setName(resultSet.getString(++paramIndex));
        task.setGroup(resultSet.getString(++paramIndex));
        task.setType(resultSet.getString(++paramIndex));
        task.setTimeoutPolicy(resultSet.getString(++paramIndex));
        task.setDependsOn(MAPPER.readValue(resultSet.getString(++paramIndex), DEPENDENCY_INFO_LIST_TYPE_REF));
        task.setProperties(MAPPER.readValue(resultSet.getString(++paramIndex), PROPERTIES_TYPE_REF));
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
