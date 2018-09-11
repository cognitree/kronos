# JBDC Store

JDBC store is an extension for store which can be used to plug in most of the database supporting JDBC to store the state of Kronos.

## Configuring JDBC Store

Update the `storeProviderConfig` section in `scheduler.yaml` to configure namespace store.

```
storeProviderConfig:
  providerClass: com.cognitree.kronos.scheduler.store.jdbc.StdJDBCStoreProvider
  config:
    connectionURL:
    driverClass:
    username:
    password:
```

Here, A [StdJDBCStoreProvider](src/main/java/com/cognitree/kronos/scheduler/store/jdbc/StdJDBCStoreProvider.java) is configured. Update the `connectionURL`, `driverClass`, `username` and `password` to connect to running jdbc backed database.
Other optional configurable parameters are
    - quartzDriverDelegate (used to configure the store used by quartz to store its data. Kronos will try to figure out quartz driver delegate from the `driverClass` if not specified explicitly. Check this out to know the list of available [quartz driver delegate](http://www.quartz-scheduler.org/documentation/quartz-2.x/configuration/ConfigJobStoreTX.html))
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)