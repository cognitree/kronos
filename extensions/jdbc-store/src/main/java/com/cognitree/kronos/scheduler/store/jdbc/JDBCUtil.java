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

import org.quartz.impl.jdbcjobstore.HSQLDBDelegate;
import org.quartz.impl.jdbcjobstore.MSSQLDelegate;
import org.quartz.impl.jdbcjobstore.PostgreSQLDelegate;
import org.quartz.impl.jdbcjobstore.StdJDBCDelegate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class JDBCUtil {
    public static final String HSQLDB_DRIVER_CLASS = "org.hsqldb.jdbcDriver";
    public static final String POSTGRESQL_DRIVER_CLASS = "org.postgresql.Driver";
    public static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    public static final String MSSQL_DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

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

    public static String getQuartzJDBCDelegate(String driverClassName) throws Exception {
        switch (driverClassName) {
            case HSQLDB_DRIVER_CLASS:
                return HSQLDBDelegate.class.getName();
            case POSTGRESQL_DRIVER_CLASS:
                return PostgreSQLDelegate.class.getName();
            case MSSQL_DRIVER_CLASS:
                return MSSQLDelegate.class.getName();
            case MYSQL_DRIVER_CLASS:
                return StdJDBCDelegate.class.getName();
            default:
                throw new Exception("unable to get the quartz driver delegate for driver class " + driverClassName);
        }
    }
}
