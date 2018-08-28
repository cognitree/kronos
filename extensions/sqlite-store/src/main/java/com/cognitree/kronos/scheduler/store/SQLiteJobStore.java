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

import com.cognitree.kronos.model.Job;
import com.cognitree.kronos.model.JobId;
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
 * A SQLite implementation of {@link JobStore}.
 */
public class SQLiteJobStore implements JobStore {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteJobStore.class);

    private static final String INSERT_JOB = "INSERT INTO jobs VALUES (?,?,?,?,?,?,?)";
    private static final String LOAD_JOB_BY_NAMESPACE = "SELECT * FROM jobs WHERE namespace = ?";
    private static final String LOAD_JOB_BY_ID = "SELECT * FROM jobs WHERE id = ? AND namespace = ?";
    private static final String LOAD_ALL_JOB_CREATED_BETWEEN = "SELECT * FROM jobs where namespace = ? " +
            "AND created_at > ? AND created_at < ?";
    private static final String LOAD_JOB_BY_NAME_CREATED_BETWEEN = "SELECT * FROM jobs WHERE workflow_name = ? " +
            "AND namespace = ? AND created_at > ? AND created_at < ?";
    private static final String LOAD_JOB_BY_NAME_TRIGGER_CREATED_BETWEEN = "SELECT * FROM jobs WHERE workflow_name = ? " +
            "AND trigger_name = ? AND namespace = ? AND created_at > ? AND created_at < ?";
    private static final String UPDATE_JOB = "UPDATE jobs SET status = ?, created_at = ?, completed_at = ? " +
            " WHERE id = ? AND namespace = ?";
    private static final String DELETE_JOB = "DELETE FROM jobs WHERE id = ? AND namespace = ?";
    private static final String DDL_CREATE_JOB_SQL = "CREATE TABLE IF NOT EXISTS jobs (" +
            "id string," +
            "workflow_name string," +
            "trigger_name string," +
            "namespace string," +
            "status string," +
            "created_at integer," +
            "completed_at integer," +
            "PRIMARY KEY(id, workflow_name, namespace)" +
            ")";
    private static final String CREATE_JOB_WORKFLOW_INDEX_SQL = "CREATE INDEX IF NOT EXISTS jobs_workflow_name_namespace_idx " +
            "on jobs (workflow_name, namespace)";
    private static final String CREATE_JOB_WORKFLOW_TRIGGER_INDEX_SQL = "CREATE INDEX IF NOT EXISTS" +
            " jobs_workflow_name_trigger_namespace_idx on jobs (workflow_name, trigger_name, namespace)";

    private BasicDataSource dataSource;

    @Override
    public void init(ObjectNode storeConfig) throws Exception {
        logger.info("Initializing SQLite job store");
        initDataSource(storeConfig);
        initJobStore();
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

    private void initJobStore() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(30);
            statement.executeUpdate(DDL_CREATE_JOB_SQL);
            statement.executeUpdate(CREATE_JOB_WORKFLOW_INDEX_SQL);
            statement.executeUpdate(CREATE_JOB_WORKFLOW_TRIGGER_INDEX_SQL);
        }
    }


    @Override
    public void store(Job job) {
        logger.debug("Received request to store job {}", job);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_JOB)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, job.getId());
            preparedStatement.setString(++paramIndex, job.getWorkflowName());
            preparedStatement.setString(++paramIndex, job.getTriggerName());
            preparedStatement.setString(++paramIndex, job.getNamespace());
            preparedStatement.setString(++paramIndex, job.getStatus().name());
            preparedStatement.setLong(++paramIndex, job.getCreatedAt());
            preparedStatement.setLong(++paramIndex, job.getCompletedAt());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error storing job {} into database", job, e);
        }
    }

    @Override
    public List<Job> load(String namespace) {
        logger.debug("Received request to get all jobs in namespace {}", namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_JOB_BY_NAMESPACE)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, namespace);
            final ResultSet resultSet = preparedStatement.executeQuery();
            final ArrayList<Job> jobs = new ArrayList<>();
            while (resultSet.next()) {
                jobs.add(getJob(resultSet));
            }
            return jobs;
        } catch (Exception e) {
            logger.error("Error fetching all jobs from database in namespace {}", namespace, e);
        }
        return Collections.emptyList();
    }

    @Override
    public Job load(JobId jobId) {
        logger.debug("Received request to get job with id {}", jobId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_JOB_BY_ID)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, jobId.getId());
            preparedStatement.setString(++paramIndex, jobId.getNamespace());
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return getJob(resultSet);
            }
        } catch (Exception e) {
            logger.error("Error fetching job from database with id {}", jobId, e);
        }
        return null;
    }

    @Override
    public List<Job> load(String namespace, long createdAfter, long createdBefore) {
        logger.debug("Received request to get all jobs under namespace {}, created after {}, created before {}",
                namespace, createdAfter, createdBefore);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_ALL_JOB_CREATED_BETWEEN)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, namespace);
            preparedStatement.setLong(++paramIndex, createdAfter);
            preparedStatement.setLong(++paramIndex, createdBefore);
            final ResultSet resultSet = preparedStatement.executeQuery();
            final ArrayList<Job> jobs = new ArrayList<>();
            while (resultSet.next()) {
                jobs.add(getJob(resultSet));
            }
            return jobs;
        } catch (Exception e) {
            logger.error("Error fetching all jobs from database under namespace {} created after {}, created before {}",
                    namespace, createdAfter, createdBefore, e);
        }
        return Collections.emptyList();
    }

    @Override
    public List<Job> loadByWorkflowName(String workflowName, String namespace, long createdAfter, long createdBefore) {
        logger.debug("Received request to get jobs with workflow name {}, namespace {}, created after {}",
                workflowName, namespace, createdAfter);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_JOB_BY_NAME_CREATED_BETWEEN)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowName);
            preparedStatement.setString(++paramIndex, namespace);
            preparedStatement.setLong(++paramIndex, createdAfter);
            preparedStatement.setLong(++paramIndex, createdBefore);
            final ResultSet resultSet = preparedStatement.executeQuery();
            final ArrayList<Job> jobs = new ArrayList<>();
            while (resultSet.next()) {
                jobs.add(getJob(resultSet));
            }
            return jobs;
        } catch (Exception e) {
            logger.error("Error fetching jobs from database with workflow name {}, namespace {}",
                    workflowName, namespace, e);
        }
        return Collections.emptyList();
    }

    @Override
    public List<Job> loadByWorkflowNameAndTrigger(String workflowName, String triggerName, String namespace,
                                                  long createdAfter, long createdBefore) {
        logger.debug("Received request to get all jobs with workflow name {} under namespace {}, triggerName {}," +
                " created after {}, created before {}", workflowName, namespace, triggerName, createdAfter, createdBefore);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_JOB_BY_NAME_TRIGGER_CREATED_BETWEEN)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowName);
            preparedStatement.setString(++paramIndex, triggerName);
            preparedStatement.setString(++paramIndex, namespace);
            preparedStatement.setLong(++paramIndex, createdAfter);
            preparedStatement.setLong(++paramIndex, createdBefore);
            final ResultSet resultSet = preparedStatement.executeQuery();
            final ArrayList<Job> jobs = new ArrayList<>();
            while (resultSet.next()) {
                jobs.add(getJob(resultSet));
            }
            return jobs;
        } catch (Exception e) {
            logger.error("Error fetching all jobs from database with workflow name {} under namespace {} " +
                    "created after {}, created before {}", workflowName, namespace, createdAfter, createdBefore, e);
        }
        return Collections.emptyList();
    }

    @Override
    public void update(Job job) {
        final JobId jobId = job.getIdentity();
        logger.info("Received request to update job to {}", job);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_JOB)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, job.getStatus().name());
            preparedStatement.setLong(++paramIndex, job.getCreatedAt());
            preparedStatement.setLong(++paramIndex, job.getCompletedAt());
            preparedStatement.setString(++paramIndex, jobId.getId());
            preparedStatement.setString(++paramIndex, jobId.getNamespace());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error updating job to {}", job, e);
        }
    }

    @Override
    public void delete(JobId jobId) {
        logger.debug("Received request to delete job with id {}", jobId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE_JOB)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, jobId.getId());
            preparedStatement.setString(++paramIndex, jobId.getNamespace());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error("Error deleting job with id {} from database", jobId, e);
        }
    }

    private Job getJob(ResultSet resultSet) throws Exception {
        int paramIndex = 0;
        Job job = new Job();
        job.setId(resultSet.getString(++paramIndex));
        job.setWorkflowName(resultSet.getString(++paramIndex));
        job.setTriggerName(resultSet.getString(++paramIndex));
        job.setNamespace(resultSet.getString(++paramIndex));
        job.setStatus(Job.Status.valueOf(resultSet.getString(++paramIndex)));
        job.setCreatedAt(resultSet.getLong(++paramIndex));
        job.setCompletedAt(resultSet.getLong(++paramIndex));
        return job;
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
