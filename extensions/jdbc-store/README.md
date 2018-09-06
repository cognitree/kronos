# JBDC Store

JDBC store is an extension for store which can be used to plug in most of the database supporting JDBC to store the state of Kronos.

## Configuring JDBC Namespace Store

Update the `namespaceStoreConfig` section in `scheduler.yaml` to configure namespace store.

```
namespaceStoreConfig:
  storeClass: com.cognitree.kronos.scheduler.store.StdJDBCNamespaceStore
  config:
    connectionURL: 
    username:
    password:
```

Here, A [StdJDBCNamespaceStore](src/main/java/com/cognitree/kronos/scheduler/store/StdJDBCNamespaceStore.java) is configured. Update the `connectionURL`, `username` and `password` to connect to running jdbc backed database.
Other configurable parameters are
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)

## Configuring JDBC Workflow Store

Update the `workflowStoreConfig` section in `scheduler.yaml` to configure workflow store.

```
workflowStoreConfig:
  storeClass: com.cognitree.kronos.scheduler.store.StdJDBCWorkflowStore
  config:
    connectionURL: 
    username:
    password:
```

Here, A [StdJDBCWorkflowStore](src/main/java/com/cognitree/kronos/scheduler/store/StdJDBCWorkflowStore.java) is configured. Update the `connectionURL`, `username` and `password` to connect to running jdbc backed database.
Other configurable parameters are
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)

## Configuring JDBC Workflow Trigger Store

Update the `workflowTriggerStoreConfig` section in `scheduler.yaml` to configure workflow trigger store.

```
workflowTriggerStoreConfig:
  storeClass: com.cognitree.kronos.scheduler.store.StdJDBCWorkflowTriggerStore
  config:
    connectionURL: 
    username:
    password:
```

Here, A [StdJDBCWorkflowTriggerStore](src/main/java/com/cognitree/kronos/scheduler/store/StdJDBCWorkflowTriggerStore.java) is configured. Update the `connectionURL`, `username` and `password` to connect to running jdbc backed database.
Other configurable parameters are
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)

## Configuring JDBC Job Store

Update the `jobStoreConfig` section in `scheduler.yaml` to configure job store storing runtime instances of workflows.

```
jobStoreConfig:
  storeClass: com.cognitree.kronos.scheduler.store.StdJDBCJobStore
  config:
    connectionURL: 
    username:
    password:
```

Here, A [StdJDBCJobStore](src/main/java/com/cognitree/kronos/scheduler/store/StdJDBCJobStore.java) is configured. Update the `connectionURL`, `username` and `password` to connect to running jdbc backed database.
Other configurable parameters are
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)

## Configuring JDBC Task Store

Update the `taskStoreConfig` section in `scheduler.yaml` to configure task store.

```
taskStoreConfig:
  storeClass: com.cognitree.kronos.scheduler.store.StdJDBCTaskStore
  config:
    connectionURL: 
    username:
    password:
```

Here, A [StdJDBCTaskStore](src/main/java/com/cognitree/kronos/scheduler/store/StdJDBCTaskStore.java) is configured. Update the `connectionURL`, `username` and `password` to connect to running jdbc backed database.
Other configurable parameters are
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)

