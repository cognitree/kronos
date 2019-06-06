# Mongo Store

Mongo store is an extension for store which can be used to plug in mongodb as the database to store the state of Kronos.

## Configuring Mongo Store

Update the `storeServiceConfig` section in `scheduler.yaml` to configure a Mongo store.

```
storeServiceConfig:
  storeServiceClass: com.cognitree.kronos.scheduler.store.mongo.MongoStoreService
  config:
    connectionString: "mongodb://localhost:27017"
```

Here, A [MongoStoreService](src/main/java/com/cognitree/kronos/scheduler/store/mongo/MongoStoreService.java) is configured.
Other optional configurable parameters are
    - connectionString (mongo db connection string, defaults to `mongodb://localhost:27017`)
    - user (username to use for authentication)
    - password (password of the user to use for authentication)
    - authDatabase (authentication database to use for authentication)
    - schedulerDatabase (name of the database to use for storing scheduler state)
