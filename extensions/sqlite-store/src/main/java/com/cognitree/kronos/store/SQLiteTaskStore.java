package com.cognitree.kronos.store;

import com.cognitree.kronos.model.Task;
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

public class SQLiteTaskStore implements TaskStore {
    private static final Logger logger = LoggerFactory.getLogger(SQLiteTaskStore.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String INSERT_REPLACE_TASK = "INSERT OR REPLACE INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private static final String UPDATE_TASK = "UPDATE tasks SET " +
            "status = ?," +
            "status_message = ?," +
            "runtime_properties = ?," +
            "submitted_at = ?," +
            "completed_at = ?" +
            " WHERE id = ?";
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
            "runtime_properties string," +
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
            preparedStatement.setString(1, task.getId());
            preparedStatement.setString(2, task.getName());
            preparedStatement.setString(3, task.getGroup());
            preparedStatement.setString(4, task.getType());
            preparedStatement.setString(5, task.getTimeoutPolicy());
            preparedStatement.setString(6, MAPPER.writeValueAsString(task.getDependsOn()));
            preparedStatement.setString(7, MAPPER.writeValueAsString(task.getProperties()));
            preparedStatement.setString(8, MAPPER.writeValueAsString(task.getRuntimeProperties()));
            preparedStatement.setString(9, task.getStatus().name());
            preparedStatement.setString(10, task.getStatusMessage());
            preparedStatement.setLong(11, task.getCreatedAt());
            preparedStatement.setLong(12, task.getSubmittedAt());
            preparedStatement.setLong(13, task.getCompletedAt());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("Error storing task {} into database", task, e);
        }
    }

    @Override
    public void update(Task task) {
        logger.debug("Received request to update task {}", task);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_TASK)) {
            preparedStatement.setString(1, task.getStatus().name());
            preparedStatement.setString(2, task.getStatusMessage());
            preparedStatement.setString(3, MAPPER.writeValueAsString(task.getRuntimeProperties()));
            preparedStatement.setLong(4, task.getSubmittedAt());
            preparedStatement.setLong(5, task.getCompletedAt());
            preparedStatement.setString(6, task.getId());
            preparedStatement.execute();
        } catch (Exception e) {
            logger.error("error updating task {} into database", task, e);
        }
    }

    @Override
    public Task load(String taskId, String taskGroup) {
        logger.debug("Received request to get task with id {}, group {}", taskId, taskGroup);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_TASK)) {
            preparedStatement.setString(1, taskId);
            preparedStatement.setString(2, taskGroup);
            final ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.first()) {
                return getTask(resultSet);
            }
        } catch (Exception e) {
            logger.error("Error fetching task from database", e);
        }
        return null;
    }

    @Override
    public List<Task> load(List<Task.Status> statuses) {
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
            logger.error("Error fetching task from database", e);
            return Collections.emptyList();
        }
    }

    @Override
    public List<Task> load(String taskName, String taskGroup, long createdBefore, long createdAfter) {
        logger.debug("Received request to get all tasks with name {}, group {}, created before {}, " +
                "created after {}", taskName, taskGroup, createdBefore, createdAfter);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(LOAD_TASK_BY_NAME_GROUP)) {
            preparedStatement.setString(1, taskName);
            preparedStatement.setString(2, taskGroup);
            preparedStatement.setLong(3, createdBefore);
            preparedStatement.setLong(4, createdAfter);
            final ResultSet resultSet = preparedStatement.executeQuery();
            List<Task> tasks = new ArrayList<>();
            while (resultSet.next()) {
                tasks.add(getTask(resultSet));
            }
            return tasks;
        } catch (Exception e) {
            logger.error("Error fetching task from database", e);
            return Collections.emptyList();
        }
    }

    private Task getTask(ResultSet resultSet) throws Exception {
        Task task = new Task();
        task.setId(resultSet.getString(1));
        task.setName(resultSet.getString(2));
        task.setGroup(resultSet.getString(3));
        task.setType(resultSet.getString(4));
        task.setTimeoutPolicy(resultSet.getString(5));
        task.setDependsOn(MAPPER.readValue(resultSet.getString(6), DEPENDENCY_INFO_LIST_TYPE_REF));
        task.setProperties(MAPPER.readValue(resultSet.getString(7), PROPERTIES_TYPE_REF));
        task.setRuntimeProperties(MAPPER.readValue(resultSet.getString(8), ObjectNode.class));
        task.setStatus(Task.Status.valueOf(resultSet.getString(9)));
        task.setStatusMessage(resultSet.getString(10));
        task.setCreatedAt(resultSet.getLong(11));
        task.setSubmittedAt(resultSet.getLong(12));
        task.setCompletedAt(resultSet.getLong(13));
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
