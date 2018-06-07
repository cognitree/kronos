# SQLite Task Store

SQLite task store is an extension for task store which uses [SQLite](https://www.sqlite.org) database to store the task and their state.

## Configuring SQLite Task Store

Update the `taskStoreConfig` section in `scheduler.yaml` to configure to sqlite task store.

```
taskStoreConfig:
  taskStoreClass: com.cognitree.kronos.store.SQLiteTaskStore
  config:
    connectionURL: jdbc:sqlite:sample.db
    username:
    password:
```

Here, A [SQLiteTaskStore](src/main/java/com/cognitree/kronos/store/SQLiteTaskStore.java) is configured. Update the `connectionURL`, `username` and `password` to connect to running sqlite db.
Other configurable parameters are
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)

