# SQLite Store

SQLite store is an extension for store which uses [SQLite](https://www.sqlite.org) database to store the state of Kronos.

## Configuring SQLite Namespace Store

Update the `namespaceStoreConfig` section in `scheduler.yaml` to configure to sqlite namespace store.

```
namespaceStoreConfig:
  storeClass: com.cognitree.kronos.scheduler.store.SQLiteNamespaceStore
  config:
    connectionURL: jdbc:sqlite:kronos.db
    username:
    password:
```

Here, A [SQLiteNamespaceStore](src/main/java/com/cognitree/kronos/scheduler/store/SQLiteNamespaceStore.java) is configured. Update the `connectionURL`, `username` and `password` to connect to running sqlite db.
Other configurable parameters are
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)

## Configuring SQLite Task Definition Store

Update the `taskDefinitionStoreConfig` section in `scheduler.yaml` to configure to sqlite task definition store.

```
taskDefinitionStoreConfig:
  storeClass: com.cognitree.kronos.scheduler.store.SQLiteTaskDefinitionStore
  config:
    connectionURL: jdbc:sqlite:kronos.db
    username:
    password:
```

Here, A [SQLiteTaskDefinitionStore](src/main/java/com/cognitree/kronos/scheduler/store/SQLiteTaskDefinitionStore.java) is configured. Update the `connectionURL`, `username` and `password` to connect to running sqlite db.
Other configurable parameters are
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)

## Configuring SQLite Workflow Store

Update the `workflowStoreConfig` section in `scheduler.yaml` to configure to sqlite workflow store.

```
workflowStoreConfig:
  storeClass: com.cognitree.kronos.scheduler.store.SQLiteWorkflowStore
  config:
    connectionURL: jdbc:sqlite:kronos.db
    username:
    password:
```

Here, A [SQLiteWorkflowStore](src/main/java/com/cognitree/kronos/scheduler/store/SQLiteWorkflowStore.java) is configured. Update the `connectionURL`, `username` and `password` to connect to running sqlite db.
Other configurable parameters are
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)

## Configuring SQLite Workflow Trigger Store

Update the `workflowTriggerStoreConfig` section in `scheduler.yaml` to configure to sqlite workflow trigger store.

```
workflowTriggerStoreConfig:
  storeClass: com.cognitree.kronos.scheduler.store.SQLiteWorkflowTriggerStore
  config:
    connectionURL: jdbc:sqlite:kronos.db
    username:
    password:
```

Here, A [SQLiteWorkflowTriggerStore](src/main/java/com/cognitree/kronos/scheduler/store/SQLiteWorkflowTriggerStore.java) is configured. Update the `connectionURL`, `username` and `password` to connect to running sqlite db.
Other configurable parameters are
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)

## Configuring SQLite Job Store

Update the `jobStoreConfig` section in `scheduler.yaml` to configure to sqlite job store storing runtime instances of workflows.

```
jobStoreConfig:
  storeClass: com.cognitree.kronos.scheduler.store.SQLiteJobStore
  config:
    connectionURL: jdbc:sqlite:kronos.db
    username:
    password:
```

Here, A [SQLiteJobStore](src/main/java/com/cognitree/kronos/scheduler/store/SQLiteJobStore.java) is configured. Update the `connectionURL`, `username` and `password` to connect to running sqlite db.
Other configurable parameters are
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)

## Configuring SQLite Task Store

Update the `taskStoreConfig` section in `scheduler.yaml` to configure to sqlite task store.

```
taskStoreConfig:
  storeClass: com.cognitree.kronos.scheduler.store.SQLiteTaskStore
  config:
    connectionURL: jdbc:sqlite:kronos.db
    username:
    password:
```

Here, A [SQLiteTaskStore](src/main/java/com/cognitree/kronos/scheduler/store/SQLiteTaskStore.java) is configured. Update the `connectionURL`, `username` and `password` to connect to running sqlite db.
Other configurable parameters are
    - minIdleConnection (minimum number of idle connections in the pool)
    - maxIdleConnection (maximum number of idle connections in the pool)
    - maxOpenPreparedStatements (maximum number of open prepared statements)

