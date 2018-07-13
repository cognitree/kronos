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

import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.WorkflowId;
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
public class SQLiteWorkflowStore implements WorkflowStore {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteWorkflowStore.class);

    private static final String INSERT_REPLACE_WORKFLOW = "INSERT OR REPLACE INTO workflows VALUES (?,?,?,?,?)";
    private static final String LOAD_WORKFLOW = "SELECT * FROM workflows";
    private static final String LOAD_WORKFLOW_BY_ID = "SELECT * FROM workflows WHERE id = ? AND namespace = ?";
    private static final String LOAD_WORKFLOW_BY_NAME_CREATED_AFTER = "SELECT * FROM workflows WHERE name = ? AND namespace = ?" +
            " AND created_at > ? AND created_at < ?";
    private static final String LOAD_ALL_WORKFLOW_CREATED_AFTER = "SELECT * FROM workflows where created_at > ? AND created_at < ?";
    private static final String UPDATE_WORKFLOW = "UPDATE workflows SET description = ?, created_at = ? " +
            " WHERE id = ? AND namespace = ?";
    private static final String DELETE_WORKFLOW = "DELETE FROM workflows WHERE id = ? AND namespace = ?";
    private static final String DDL_CREATE_WORKFLOW_SQL = "CREATE TABLE IF NOT EXISTS workflows (" +
            "id string," +
            "name string," +
            "namespace string," +
            "description string," +
            "created_at integer," +
            "PRIMARY KEY(id)" +
            ")";
    private static final String CREATE_WORKFLOW_INDEX_SQL = "CREATE INDEX IF NOT EXISTS workflows_name_namespace_idx on workflows (name, namespace)";

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
            statement.executeUpdate(DDL_CREATE_WORKFLOW_SQL);
            statement.executeUpdate(CREATE_WORKFLOW_INDEX_SQL);
        }
    }


    @Override
    public void store(Workflow workflow) {
        logger.debug("Received request to store workflow {}", workflow);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_REPLACE_WORKFLOW)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflow.getId());
            preparedStatement.setString(++paramIndex, workflow.getName());
            preparedStatement.setString(++paramIndex, workflow.getNamespace());
            preparedStatement.setString(++paramIndex, workflow.getDescription());
            preparedStatement.setLong(++paramIndex, workflow.getCreatedAt());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error storing workflow {} into database", workflow, e);
        }
    }

    @Override
    public List<Workflow> load() {
        logger.debug("Received request to get all workflow");
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_WORKFLOW)) {
            final ResultSet resultSet = preparedStatement.executeQuery();
            final ArrayList<Workflow> workflows = new ArrayList<>();
            while (resultSet.next()) {
                workflows.add(getWorkflow(resultSet));
            }
            return workflows;
        } catch (Exception e) {
            logger.error("Error fetching all workflow from database", e);
        }
        return Collections.emptyList();
    }

    @Override
    public Workflow load(WorkflowId workflowId) {
        logger.debug("Received request to get workflow with id {}", workflowId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_WORKFLOW_BY_ID)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowId.getId());
            preparedStatement.setString(++paramIndex, workflowId.getNamespace());
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return getWorkflow(resultSet);
            }
        } catch (Exception e) {
            logger.error("Error fetching workflow from database with id {}", workflowId, e);
        }
        return null;
    }

    @Override
    public List<Workflow> load(long createdAfter, long createdBefore) {
        logger.debug("Received request to get all workflow created after {}", createdAfter);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_ALL_WORKFLOW_CREATED_AFTER)) {
            int paramIndex = 0;
            preparedStatement.setLong(++paramIndex, createdAfter);
            preparedStatement.setLong(++paramIndex, createdBefore);
            final ResultSet resultSet = preparedStatement.executeQuery();
            final ArrayList<Workflow> workflows = new ArrayList<>();
            while (resultSet.next()) {
                workflows.add(getWorkflow(resultSet));
            }
            return workflows;
        } catch (Exception e) {
            logger.error("Error fetching all workflow from database created after {}", createdAfter, e);
        }
        return Collections.emptyList();
    }

    @Override
    public List<Workflow> loadByName(String name, String namespace, long createdAfter, long createdBefore) {
        logger.debug("Received request to get workflow with name {}, namespace {}, created after {}", name, namespace, createdAfter);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_WORKFLOW_BY_NAME_CREATED_AFTER)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, name);
            preparedStatement.setString(++paramIndex, namespace);
            preparedStatement.setLong(++paramIndex, createdAfter);
            preparedStatement.setLong(++paramIndex, createdBefore);
            final ResultSet resultSet = preparedStatement.executeQuery();
            final ArrayList<Workflow> workflows = new ArrayList<>();
            while (resultSet.next()) {
                workflows.add(getWorkflow(resultSet));
            }
            return workflows;
        } catch (Exception e) {
            logger.error("Error fetching workflow from database with name {}, namespace {}", name, namespace, e);
        }
        return Collections.emptyList();
    }

    @Override
    public void update(Workflow workflow) {
        final WorkflowId workflowId = workflow.getIdentity();
        logger.debug("Received request to update workflow with id {} to {}", workflowId, workflow);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_WORKFLOW)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflow.getDescription());
            preparedStatement.setLong(++paramIndex, workflow.getCreatedAt());
            preparedStatement.setString(++paramIndex, workflowId.getId());
            preparedStatement.setString(++paramIndex, workflowId.getNamespace());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error updating workflow with id {} to {}", workflowId, workflow, e);
        }
    }

    @Override
    public void delete(WorkflowId workflowId) {
        logger.debug("Received request to delete workflow with id {}", workflowId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE_WORKFLOW)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowId.getId());
            preparedStatement.setString(++paramIndex, workflowId.getNamespace());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error("Error deleting workflow with id {} from database", workflowId, e);
        }
    }

    private Workflow getWorkflow(ResultSet resultSet) throws Exception {
        int paramIndex = 0;
        Workflow workflow = new Workflow();
        workflow.setId(resultSet.getString(++paramIndex));
        workflow.setName(resultSet.getString(++paramIndex));
        workflow.setNamespace(resultSet.getString(++paramIndex));
        workflow.setDescription(resultSet.getString(++paramIndex));
        workflow.setCreatedAt(resultSet.getLong(++paramIndex));
        return workflow;
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
