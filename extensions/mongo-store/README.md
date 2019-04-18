# Mongo Store

Mongo store is an extension for store which can be used to plug in mongodb as the database to store the state of Kronos.

## Configuring Mongo Store

Update the `storeServiceConfig` section in `scheduler.yaml` to configure a Mongo store.

```
storeServiceConfig:
  storeServiceClass: com.cognitree.kronos.scheduler.store.mongo.MongoStoreService
  config:
    host: localhost
    port: 27017
```

Here, A [MongoStoreService](src/main/java/com/cognitree/kronos/scheduler/store/mongo/MongoStoreService.java) is configured.
Other optional configurable parameters are
    - host (ip address of the machine running mongodb, defaults to `localhost`)
    - port (port of the mongodb, defaults to `27017`)
    - user (username to use for authentication)
    - password (password of the user to use for authentication)
    - authDatabase (authentication database to use for authentication)
    - schedulerDatabase (name of the database to use for storing scheduler state)
