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

import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.NamespaceId;
import com.cognitree.kronos.scheduler.store.NamespaceStore;
import com.cognitree.kronos.scheduler.store.StoreException;
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
import static com.cognitree.kronos.scheduler.store.jdbc.StdJDBCConstants.TABLE_NAMESPACES;

/**
 * A standard JDBC based implementation of {@link NamespaceStore}.
 */
public class StdJDBCNamespaceStore implements NamespaceStore {
    private static final Logger logger = LoggerFactory.getLogger(StdJDBCNamespaceStore.class);

    private static final String INSERT_NAMESPACE = "INSERT INTO " + TABLE_NAMESPACES + " VALUES (?,?)";

    private static final String LOAD_ALL_NAMESPACES = "SELECT * FROM " + TABLE_NAMESPACES;
    private static final String LOAD_NAMESPACE = "SELECT * FROM " + TABLE_NAMESPACES + " WHERE " + COL_NAME + " = ?";

    private static final String UPDATE_NAMESPACE = "UPDATE " + TABLE_NAMESPACES + " SET " + COL_DESCRIPTION
            + " = ? WHERE " + COL_NAME + " = ?";

    private static final String DELETE_NAMESPACE = "DELETE FROM " + TABLE_NAMESPACES + " WHERE " + COL_NAME + " = ?";

    private final BasicDataSource dataSource;

    public StdJDBCNamespaceStore(BasicDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void store(Namespace namespace) throws StoreException {
        logger.debug("Received request to store namespace {}", namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_NAMESPACE)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, namespace.getName());
            preparedStatement.setString(++paramIndex, namespace.getDescription());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error storing namespace {}", namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Namespace> load() throws StoreException {
        logger.debug("Received request to get all namespaces");
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_ALL_NAMESPACES)) {
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<Namespace> namespaces = new ArrayList<>();
            while (resultSet.next()) {
                namespaces.add(getNamespace(resultSet));
            }
            return namespaces;
        } catch (Exception e) {
            logger.error("Error fetching all namespaces", e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public Namespace load(NamespaceId namespaceId) throws StoreException {
        logger.debug("Received request to load namespace with id {}", namespaceId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_NAMESPACE)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, namespaceId.getName());
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return getNamespace(resultSet);
            }
        } catch (Exception e) {
            logger.error("Error fetching namespace with id {}", namespaceId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
        return null;
    }

    @Override
    public void update(Namespace namespace) throws StoreException {
        logger.debug("Received request to update namespace to {}", namespace);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_NAMESPACE)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, namespace.getDescription());
            preparedStatement.setString(++paramIndex, namespace.getName());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error updating namespace to {}", namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void delete(NamespaceId namespaceId) throws StoreException {
        logger.debug("Received request to delete namespace with id {}", namespaceId);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE_NAMESPACE)) {
            int paramIndex = 0;
            preparedStatement.setString(++paramIndex, namespaceId.getName());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            logger.error("Error deleting namespace with id {}", namespaceId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    private Namespace getNamespace(ResultSet resultSet) throws Exception {
        int paramIndex = 0;
        Namespace namespace = new Namespace();
        namespace.setName(resultSet.getString(++paramIndex));
        namespace.setDescription(resultSet.getString(++paramIndex));
        return namespace;
    }
}
