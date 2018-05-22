# File Reader

File reader is an extension for task definition reader to load task definitions from yaml file available in classpath

## Configuring File Reader

Update the `readerConfig` section in `app.yaml` to use to file reader.

```
readerConfig:
  # name to be assigned to the reader
  filereader:
    # fully qualified classname of the TaskDefinitionReader.java implementation
    readerClass: com.cognitree.tasks.scheduler.readers.FileTaskDefinitionReader
    # additional configuration
    config:
      source: task-definitions.yaml
    # interval in cron to look for changes in the task definition file
    schedule: 0 0/1 * 1/1 * ? *
```

Here, A [FileTaskDefinitionReader](src/main/java/com/cognitree/tasks/scheduler/readers/FileTaskDefinitionReader.java) is configured with a cron schedule to run every minute and load task definitions from `task-defintion.yaml` file in classpath.