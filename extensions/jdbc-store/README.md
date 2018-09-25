# JBDC Store

JDBC store is an extension for store which can be used to plug in most of the database supporting JDBC to store the state of Kronos.

## Configuring JDBC Store

Update the `storeServiceConfig` section in `scheduler.yaml` to configure a JDBC store.

```
storeServiceConfig:
  storeServiceClass: com.cognitree.kronos.scheduler.store.jdbc.StdJDBCStoreService
  config:
    connectionURL: jdbc:hsqldb:hsql://localhost/tmp/kronos
    driverClass: org.hsqldb.jdbcDriver
    username: kronos
    password: kronos123
```

Here, A [StdJDBCStoreService](src/main/java/com/cognitree/kronos/scheduler/store/jdbc/StdJDBCStoreService.java) is configured. Update the `connectionURL`, `driverClass`, `username` and `password` to connect to running jdbc backed database.
Other optional configurable parameters are
    - quartzDriverDelegate (used to configure the store used by quartz to store its data. Kronos will try to figure out quartz driver delegate from the `driverClass` if not specified explicitly. Check this out to know the list of available [quartz driver delegate](http://www.quartz-scheduler.org/documentation/quartz-2.x/configuration/ConfigJobStoreTX.html))
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)
