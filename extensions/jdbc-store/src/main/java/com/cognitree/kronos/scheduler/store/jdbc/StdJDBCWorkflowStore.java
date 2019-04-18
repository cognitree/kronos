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

import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.Workflow.WorkflowTask;
import com.cognitree.kronos.scheduler.model.WorkflowId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_DESCRIPTION;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_NAME;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_NAMESPACE;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_TASKS;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.TABLE_WORKFLOWS;

/**
 * A standard JDBC based implementation of {@link WorkflowStore}.
 */
public class StdJDBCWorkflowStore implements WorkflowStore {
    private static final Logger logger = LoggerFactory.getLogger(StdJDBCWorkflowStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String INSERT_WORKFLOW = "INSERT INTO " + TABLE_WORKFLOWS + " VALUES (?,?,?,?)";

    private static final String LOAD_WORKFLOW = "SELECT * FROM " + TABLE_WORKFLOWS + " WHERE "
            + COL_NAME + " = ? AND " + COL_NAMESPACE + " = ?";
    private static final String LOAD_ALL_WORKFLOW_BY_NAMESPACE = "SELECT * FROM " + TABLE_WORKFLOWS + " " + "WHERE "
            + COL_NAMESPACE + " = ?";

    private static final String UPDATE_WORKFLOW = "UPDATE " + TABLE_WORKFLOWS + " set " + COL_DESCRIPTION + " = ?, " +
            " " + COL_TASKS + " = ? WHERE " + COL_NAME + " = ? AND " + COL_NAMESPACE + " = ?";

    private static final String DELETE_WORKFLOW = "DELETE FROM " + TABLE_WORKFLOWS + " WHERE "
            + COL_NAME + " = ? " + "AND " + COL_NAMESPACE + " = ?";

    private static final TypeReference<List<WorkflowTask>> WORKFLOW_TASK_LIST_TYPE_REF =
            new TypeReference<List<WorkflowTask>>() {
            };

    private final BasicDataSource dataSource;

    public StdJDBCWorkflowStore(BasicDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void store(Workflow workflow) throws StoreException {
        logger.debug("Received request to store workflow {}", workflow);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_WORKFLOW)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflow.getName());
            preparedStatement.setString(++paramIndex, workflow.getNamespace());
            preparedStatement.setString(++paramIndex, workflow.getDescription());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(workflow.getTasks()));
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error storing workflow {}", workflow, e);
            throw new StoreException(e);
        }
    }

    @Override
    public List<Workflow> load(String namespace) throws StoreException {
        logger.debug("Received request to get all workflow under namespace {}", namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_ALL_WORKFLOW_BY_NAMESPACE)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, namespace);
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<Workflow> workflows = new ArrayList<>();
            while (resultSet.next()) {
                workflows.add(getWorkflow(resultSet));
            }
            return workflows;
        } catch (Exception e) {
            logger.error("Error fetching all workflow under namespace {}", namespace, e);
            throw new StoreException(e);
        }
    }

    @Override
    public Workflow load(WorkflowId workflowId) throws StoreException {
        logger.debug("Received request to load workflow with id {}", workflowId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_WORKFLOW)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowId.getName());
            preparedStatement.setString(++paramIndex, workflowId.getNamespace());
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return getWorkflow(resultSet);
            }
        } catch (Exception e) {
            logger.error("Error fetching workflow with id {}", workflowId, e);
            throw new StoreException(e);
        }
        return null;
    }

    @Override
    public void update(Workflow workflow) throws StoreException {
        logger.debug("Received request to update workflow to {}", workflow);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_WORKFLOW)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflow.getDescription());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(workflow.getTasks()));
            preparedStatement.setString(++paramIndex, workflow.getName());
            preparedStatement.setString(++paramIndex, workflow.getNamespace());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error updating workflow to {}", workflow, e);
            throw new StoreException(e);
        }
    }

    @Override
    public void delete(WorkflowId workflowId) throws StoreException {
        logger.debug("Received request to delete workflow with id {}", workflowId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE_WORKFLOW)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowId.getName());
            preparedStatement.setString(++paramIndex, workflowId.getNamespace());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error("Error deleting workflow with id {}", workflowId, e);
            throw new StoreException(e);
        }
    }

    private Workflow getWorkflow(ResultSet resultSet) throws Exception {
        int paramIndex = 0;
        Workflow workflow = new Workflow();
        workflow.setName(resultSet.getString(++paramIndex));
        workflow.setNamespace(resultSet.getString(++paramIndex));
        workflow.setDescription(resultSet.getString(++paramIndex));
        workflow.setTasks(MAPPER.readValue(resultSet.getString(++paramIndex), WORKFLOW_TASK_LIST_TYPE_REF));
        return workflow;
    }
}
