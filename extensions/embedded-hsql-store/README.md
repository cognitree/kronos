# Embedded HSQL Store

Embedded HSQL store is an extension for store which runs an embedded HSQLDB to store the state of Kronos.

## Configuring Embedded HSQL Store

Update the `storeServiceConfig` section in `scheduler.yaml` to configure embedded HSQLDB store.

```
storeServiceConfig:
  storeServiceClass: com.cognitree.kronos.scheduler.store.jdbc.EmbeddedHSQLStoreService
  config:
    # directory to keep the Kronos data
    dbPath: /tmp
    # database username (used while creating and accessing the database)
    username:
    # database password (used while creating and accessing the database)
    password:
```

Here, A [EmbeddedHSQLStoreService](src/main/java/com/cognitree/kronos/scheduler/store/jdbc/EmbeddedHSQLStoreService.java) is configured. Update the `dbPath`, `username` and `password` to be used to run embedded HSQL database.
Other optional configurable parameters are
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)
