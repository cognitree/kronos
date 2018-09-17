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

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.TaskStore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_COMPLETED_AT;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_CONTEXT;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_JOB_ID;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_NAME;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_NAMESPACE;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_STATUS;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_STATUS_MESSAGE;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_SUBMITTED_AT;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.TABLE_TASKS;

/**
 * A standard JDBC based implementation of {@link TaskStore}.
 */
public class StdJDBCTaskStore implements TaskStore {
    private static final Logger logger = LoggerFactory.getLogger(StdJDBCTaskStore.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String INSERT_TASK = "INSERT INTO " + TABLE_TASKS + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private static final String UPDATE_TASK = "UPDATE " + TABLE_TASKS + " SET " + COL_STATUS + " = ?, "
            + COL_STATUS_MESSAGE + " = ?, " + COL_SUBMITTED_AT + " = ?, " + COL_COMPLETED_AT + " = ?, "
            + COL_CONTEXT + " = ? WHERE " + COL_NAME + " = ? AND " + COL_JOB_ID + " = ? AND " + COL_NAMESPACE + " = ?";
    private static final String LOAD_ALL_TASKS_BY_NAMESPACE = "SELECT * FROM " + TABLE_TASKS + " WHERE "
            + COL_NAMESPACE + " = ?";
    private static final String LOAD_TASK = "SELECT * FROM " + TABLE_TASKS + " WHERE " + COL_NAME + " = ? AND "
            + COL_JOB_ID + " = ? AND " + COL_NAMESPACE + " = ?";
    private static final String LOAD_TASK_BY_STATUS = "SELECT * FROM " + TABLE_TASKS + " WHERE " + COL_STATUS
            + " IN ($statuses)";
    private static final String LOAD_TASK_BY_JOB_ID = "SELECT * FROM " + TABLE_TASKS + " WHERE "
            + COL_JOB_ID + " = ? AND " + COL_NAMESPACE + " = ?";
    private static final String DELETE_TASK = "DELETE FROM " + TABLE_TASKS + " WHERE "
            + COL_NAME + " = ? AND " + COL_JOB_ID + " = ? AND " + COL_NAMESPACE + " = ?";

    private static final TypeReference<Map<String, Object>> PROPERTIES_TYPE_REF =
            new TypeReference<Map<String, Object>>() {
            };
    private static final TypeReference<List<String>> DEPENDS_ON_TYPE_REF =
            new TypeReference<List<String>>() {
            };

    private final BasicDataSource dataSource;

    public StdJDBCTaskStore(BasicDataSource dataSource) {
        this.dataSource = dataSource;
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
        Task task = new Task();
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
}
