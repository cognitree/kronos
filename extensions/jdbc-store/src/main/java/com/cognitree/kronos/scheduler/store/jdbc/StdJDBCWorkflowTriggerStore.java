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

import com.cognitree.kronos.scheduler.model.Schedule;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.model.WorkflowTriggerId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_ENABLED;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_END_AT;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_NAME;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_NAMESPACE;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_SCHEDULE;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_START_AT;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.COL_WORKFLOW_NAME;
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.TABLE_WORKFLOW_TRIGGERS;

/**
 * A standard JDBC based implementation of {@link WorkflowTriggerStore}.
 */
public class StdJDBCWorkflowTriggerStore implements WorkflowTriggerStore {
    private static final Logger logger = LoggerFactory.getLogger(StdJDBCWorkflowTriggerStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String INSERT_WORKFLOW_TRIGGER = "INSERT INTO " + TABLE_WORKFLOW_TRIGGERS
            + " VALUES (?,?,?,?,?,?,?)";
    private static final String LOAD_ALL_WORKFLOW_TRIGGER_BY_NAMESPACE = "SELECT * FROM " + TABLE_WORKFLOW_TRIGGERS
            + " WHERE " + COL_NAMESPACE + " = ?";
    private static final String LOAD_ALL_WORKFLOW_TRIGGER_BY_WORKFLOW_NAME = "SELECT * FROM " + TABLE_WORKFLOW_TRIGGERS
            + " WHERE " + COL_WORKFLOW_NAME + " = ? AND " + COL_NAMESPACE + " = ?";
    private static final String UPDATE_WORKFLOW_TRIGGER = "UPDATE " + TABLE_WORKFLOW_TRIGGERS + " set " + COL_START_AT
            + " = ?, " + COL_SCHEDULE + " = ?," + " " + COL_END_AT + " = ?, " + COL_ENABLED
            + " = ? where " + COL_NAME + " = ? AND " + COL_WORKFLOW_NAME + " = ? AND " + COL_NAMESPACE + " = ?";
    private static final String DELETE_WORKFLOW_TRIGGER = "DELETE FROM " + TABLE_WORKFLOW_TRIGGERS + " where "
            + COL_NAME + " = ? " + "AND " + COL_WORKFLOW_NAME + " = ? AND " + COL_NAMESPACE + " = ?";
    private static final String LOAD_WORKFLOW_TRIGGER = "SELECT * FROM " + TABLE_WORKFLOW_TRIGGERS + " where "
            + COL_NAME + " = ? " + "AND " + COL_WORKFLOW_NAME + " = ? AND " + COL_NAMESPACE + " = ?";

    private final BasicDataSource dataSource;

    public StdJDBCWorkflowTriggerStore(BasicDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void store(WorkflowTrigger workflowTrigger) throws StoreException {
        logger.debug("Received request to store workflow trigger {}", workflowTrigger);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_WORKFLOW_TRIGGER)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowTrigger.getName());
            preparedStatement.setString(++paramIndex, workflowTrigger.getWorkflow());
            preparedStatement.setString(++paramIndex, workflowTrigger.getNamespace());
            JDBCUtil.setLong(preparedStatement, ++paramIndex, workflowTrigger.getStartAt());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(workflowTrigger.getSchedule()));
            JDBCUtil.setLong(preparedStatement, ++paramIndex, workflowTrigger.getEndAt());
            preparedStatement.setBoolean(++paramIndex, workflowTrigger.isEnabled());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error storing workflow trigger {} into database", workflowTrigger, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<WorkflowTrigger> load(String namespace) throws StoreException {
        logger.debug("Received request to get all workflow triggers in namespace {}", namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_ALL_WORKFLOW_TRIGGER_BY_NAMESPACE)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, namespace);
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<WorkflowTrigger> workflowTriggers = new ArrayList<>();
            while (resultSet.next()) {
                workflowTriggers.add(getWorkflowTrigger(resultSet));
            }
            return workflowTriggers;
        } catch (Exception e) {
            logger.error("Error fetching workflow triggers from database in namespace {}", namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<WorkflowTrigger> loadByWorkflowName(String workflowName, String namespace) throws StoreException {
        logger.debug("Received request to get all workflow triggers with workflow name {} in namespace {}",
                workflowName, namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_ALL_WORKFLOW_TRIGGER_BY_WORKFLOW_NAME)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, workflowName);
            preparedStatement.setString(++paramIndex, namespace);
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<WorkflowTrigger> workflowTriggers = new ArrayList<>();
            while (resultSet.next()) {
                workflowTriggers.add(getWorkflowTrigger(resultSet));
            }
            return workflowTriggers;
        } catch (Exception e) {
            logger.error("Error fetching workflow triggers from database with workflow name {} in namespace {}",
                    workflowName, namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public WorkflowTrigger load(WorkflowTriggerId triggerId) throws StoreException {
        logger.debug("Received request to load workflow trigger with id {}", triggerId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_WORKFLOW_TRIGGER)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, triggerId.getName());
            preparedStatement.setString(++paramIndex, triggerId.getWorkflow());
            preparedStatement.setString(++paramIndex, triggerId.getNamespace());
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return getWorkflowTrigger(resultSet);
            }
        } catch (Exception e) {
            logger.error("Error fetching workflow trigger with id {} from database", triggerId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
        return null;
    }

    @Override
    public void update(WorkflowTrigger workflowTrigger) throws StoreException {
        logger.debug("Received request to update workflow trigger to {}", workflowTrigger);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_WORKFLOW_TRIGGER)) {
            int paramIndex = 0;
            JDBCUtil.setLong(preparedStatement, ++paramIndex, workflowTrigger.getStartAt());
            preparedStatement.setString(++paramIndex, MAPPER.writeValueAsString(workflowTrigger.getSchedule()));
            JDBCUtil.setLong(preparedStatement, ++paramIndex, workflowTrigger.getEndAt());
            preparedStatement.setBoolean(++paramIndex, workflowTrigger.isEnabled());
            preparedStatement.setString(++paramIndex, workflowTrigger.getName());
            preparedStatement.setString(++paramIndex, workflowTrigger.getWorkflow());
            preparedStatement.setString(++paramIndex, workflowTrigger.getNamespace());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error updating workflow trigger {} into database", workflowTrigger, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void delete(WorkflowTriggerId triggerId) throws StoreException {
        logger.debug("Received request to delete workflow trigger with id {}", triggerId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE_WORKFLOW_TRIGGER)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, triggerId.getName());
            preparedStatement.setString(++paramIndex, triggerId.getWorkflow());
            preparedStatement.setString(++paramIndex, triggerId.getNamespace());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error("Error delete workflow trigger with id {} from database", triggerId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    private WorkflowTrigger getWorkflowTrigger(ResultSet resultSet) throws Exception {
        int paramIndex = 0;
        WorkflowTrigger workflowTrigger = new WorkflowTrigger();
        workflowTrigger.setName(resultSet.getString(++paramIndex));
        workflowTrigger.setWorkflow(resultSet.getString(++paramIndex));
        workflowTrigger.setNamespace(resultSet.getString(++paramIndex));
        workflowTrigger.setStartAt(JDBCUtil.getLong(resultSet, ++paramIndex));
        workflowTrigger.setSchedule(MAPPER.readValue(resultSet.getString(++paramIndex), Schedule.class));
        workflowTrigger.setEndAt(JDBCUtil.getLong(resultSet, ++paramIndex));
        workflowTrigger.setEnabled(resultSet.getBoolean(++paramIndex));
        return workflowTrigger;
    }
}
