package com.cognitree.kronos.scheduler.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class SQLiteUtil {

    public static void setLong(PreparedStatement preparedStatement, int paramIndex, Long value) throws SQLException {
        if (value != null) {
            preparedStatement.setLong(paramIndex, value);
        } else {
            preparedStatement.setNull(paramIndex, Types.INTEGER);
        }
    }

    public static Long getLong(ResultSet resultSet, int paramIndex) throws SQLException {
        final Long value = resultSet.getLong(paramIndex);
        if (resultSet.wasNull()) {
            return null;
        } else {
            return value;
        }
    }
}
