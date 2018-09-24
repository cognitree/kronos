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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_COMPLETED_AT;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_CONTEXT;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_CREATED_AT;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_JOB_ID;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_NAME;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_NAMESPACE;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_STATUS;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_STATUS_MESSAGE;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_SUBMITTED_AT;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_WORKFLOW_NAME;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.TABLE_TASKS;

/**
 * A standard JDBC based implementation of {@link TaskStore}.
 */
public class StdJDBCTaskStore implements TaskStore {
    private static final Logger logger = LoggerFactory.getLogger(StdJDBCTaskStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String INSERT_TASK = "INSERT INTO " + TABLE_TASKS + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String LOAD_TASK = "SELECT * FROM " + TABLE_TASKS + " WHERE " + COL_NAME + " = ? AND "
            + COL_JOB_ID + " = ? AND " + COL_WORKFLOW_NAME + " = ? AND " + COL_NAMESPACE + " = ?";
    private static final String LOAD_ALL_TASKS_BY_NAMESPACE = "SELECT * FROM " + TABLE_TASKS + " WHERE "
            + COL_NAMESPACE + " = ?";
    private static final String LOAD_TASK_BY_STATUS = "SELECT * FROM " + TABLE_TASKS + " WHERE " + COL_STATUS
            + " IN ($statuses)";
    private static final String LOAD_TASK_BY_JOB_ID = "SELECT * FROM " + TABLE_TASKS + " WHERE "
            + COL_JOB_ID + " = ? AND " + COL_WORKFLOW_NAME + " = ? AND " + COL_NAMESPACE + " = ?";

    private static final String GROUP_BY_STATUS_TASK_CREATED_BETWEEN = "SELECT STATUS, COUNT(*) FROM " + TABLE_TASKS
            + " WHERE " + COL_NAMESPACE + " = ? AND " + COL_CREATED_AT + " > ? AND " + COL_CREATED_AT + " < ? " +
            "GROUP BY " + COL_STATUS;
    private static final String GROUP_BY_STATUS_TASK_CREATED_BETWEEN_FOR_WORKFLOW = "SELECT STATUS, COUNT(*) FROM "
            + TABLE_TASKS + " WHERE " + COL_WORKFLOW_NAME + " = ? AND " + COL_NAMESPACE + " = ? AND "
            + COL_CREATED_AT + " > ? AND " + COL_CREATED_AT + " < ? " + "GROUP BY " + COL_STATUS;

    private static final String UPDATE_TASK = "UPDATE " + TABLE_TASKS + " SET " + COL_STATUS + " = ?, "
            + COL_STATUS_MESSAGE + " = ?, " + COL_SUBMITTED_AT + " = ?, " + COL_COMPLETED_AT + " = ?, "
            + COL_CONTEXT + " = ? WHERE " + COL_NAME + " = ? AND " + COL_JOB_ID + " = ? " +
            "AND " + COL_WORKFLOW_NAME + " = ? AND " + COL_NAMESPACE + " = ?";

    private static final String DELETE_TASK = "DELETE FROM " + TABLE_TASKS + " WHERE " + COL_NAME + " = ? AND "
            + COL_JOB_ID + " = ? AND " + COL_WORKFLOW_NAME + " = ? AND " + COL_NAMESPACE + " = ?";

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
            preparedStatement.setString(++paramIndex, task.getWorkflow());
            preparedStatement.setString(++paramIndex, task.getNamespace());
            preparedStatement.setString(++paramIndex, task.getType());
            preparedStatement.setLong(++paramIndex, task.getMaxExecutionTimeInMs());
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
            logger.error("Error storing task {}", task, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Task> load(String namespace) throws StoreException {
        logger.debug("Received request to get all tasks under namespace {}", namespace);
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
            logger.error("Error loading all tasks under namespace {}", namespace, e);
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
            preparedStatement.setString(++paramIndex, taskId.getWorkflow());
            preparedStatement.setString(++paramIndex, taskId.getNamespace());
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return getTask(resultSet);
            }
        } catch (Exception e) {
            logger.error("Error loading task with id {}", taskId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
        return null;
    }

    @Override
    public List<Task> loadByJobIdAndWorkflowName(String jobId, String workflowName, String namespace) throws StoreException {
        logger.debug("Received request to get all tasks with job id {}, workflow name {}, namespace {}",
                jobId, workflowName, namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_TASK_BY_JOB_ID)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, jobId);
            preparedStatement.setString(++paramIndex, workflowName);
            preparedStatement.setString(++paramIndex, namespace);
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<Task> tasks = new ArrayList<>();
            while (resultSet.next()) {
                tasks.add(getTask(resultSet));
            }
            return tasks;
        } catch (Exception e) {
            logger.error("Error fetching tasks with job id {}, workflow name {}, namespace {}",
                    jobId, workflowName, namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Task> loadByStatus(List<Status> statuses, String namespace) throws StoreException {
        logger.debug("Received request to get all tasks with status in {} under namespace {}", statuses, namespace);
        String placeHolders = String.join(",", Collections.nCopies(statuses.size(), "?"));
        final String sqlQuery = LOAD_TASK_BY_STATUS.replace("$statuses", placeHolders);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
            int paramIndex = 0;
            for (Status status : statuses) {
                preparedStatement.setString(++paramIndex, status.name());
            }
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<Task> tasks = new ArrayList<>();
            while (resultSet.next()) {
                tasks.add(getTask(resultSet));
            }
            return tasks;
        } catch (Exception e) {
            logger.error("Error fetching task with status in {} under namespace {}", statuses, namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public Map<Status, Integer> groupByStatus(String namespace, long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to get all tasks under namespace {}", namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(GROUP_BY_STATUS_TASK_CREATED_BETWEEN)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, namespace);
            preparedStatement.setLong(++paramIndex, createdAfter);
            preparedStatement.setLong(++paramIndex, createdBefore);
            final ResultSet resultSet = preparedStatement.executeQuery();
            Map<Status, Integer> statusMap = new HashMap<>();
            while (resultSet.next()) {
                statusMap.put(Status.valueOf(resultSet.getString(1)), resultSet.getInt(2));
            }
            return statusMap;
        } catch (Exception e) {
            logger.error("Error loading all tasks under namespace {}", namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public Map<Status, Integer> groupByStatusForWorkflowName(String workflowName, String namespace,
                                                             long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to group tasks by status having workflow name {} under namespace {}," +
                "created after {}, created before {}", workflowName, namespace, createdAfter, createdBefore);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(GROUP_BY_STATUS_TASK_CREATED_BETWEEN_FOR_WORKFLOW)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowName);
            preparedStatement.setString(++paramIndex, namespace);
            preparedStatement.setLong(++paramIndex, createdAfter);
            preparedStatement.setLong(++paramIndex, createdBefore);
            final ResultSet resultSet = preparedStatement.executeQuery();
            Map<Status, Integer> statusMap = new HashMap<>();
            while (resultSet.next()) {
                statusMap.put(Status.valueOf(resultSet.getString(1)), resultSet.getInt(2));
            }
            return statusMap;
        } catch (Exception e) {
            logger.error("Error grouping tasks by status having workflow name {} under namespace {}," +
                    "created after {}, created before {}", workflowName, namespace, createdAfter, createdBefore, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void update(Task task) throws StoreException {
        logger.debug("Received request to update task to {}", task);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_TASK)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, task.getStatus().name());
            preparedStatement.setString(++paramIndex, task.getStatusMessage());
            JDBCUtil.setLong(preparedStatement, ++paramIndex, task.getSubmittedAt());
            JDBCUtil.setLong(preparedStatement, ++paramIndex, task.getCompletedAt());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(task.getContext()));
            preparedStatement.setString(++paramIndex, task.getName());
            preparedStatement.setString(++paramIndex, task.getJob());
            preparedStatement.setString(++paramIndex, task.getWorkflow());
            preparedStatement.setString(++paramIndex, task.getNamespace());
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
            preparedStatement.setString(++paramIndex, taskId.getWorkflow());
            preparedStatement.setString(++paramIndex, taskId.getNamespace());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error("Error deleting task with id {}", taskId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    private Task getTask(ResultSet resultSet) throws Exception {
        int paramIndex = 0;
        Task task = new Task();
        task.setName(resultSet.getString(++paramIndex));
        task.setJob(resultSet.getString(++paramIndex));
        task.setWorkflow(resultSet.getString(++paramIndex));
        task.setNamespace(resultSet.getString(++paramIndex));
        task.setType(resultSet.getString(++paramIndex));
        task.setMaxExecutionTimeInMs(resultSet.getLong(++paramIndex));
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
