# Spark Handler

Spark handler is an extension for handler to submit a Spark job to a Spark cluster and track its status.

## Configuring Spark Handler

Update the `taskHandlerConfig` section in `executor.yaml` to configure to the Spark handler.

```
taskHandlerConfig:
  # type of task to be handled by handler
  spark:
    # handler implementation used to handle task of type spark
    handlerClass: com.cognitree.kronos.executor.handlers.SparkHandler
    # max parallel tasks handler is allowed to execute at any point of time
    maxParallelTasks: 4
```

Here, A [SparkHandler](src/main/java/com/cognitree/kronos/executor/handlers/SparkHandler.java) is configured for task type `spark`. Tasks of type `spark` will be executed by Spark handler.

## Configurable Task Properties

A number of properties can be passed along with the task to the handler which is used to execute the task.

Configurable properties supported by Spark Handler are as below:

| KEY              | DESCRIPTION                                                | TYPE         | DEFAULT      | MANDATORY |
|------------------|------------------------------------------------------------|--------------|--------------|-----------|
| sparkVersion     | client Spark version                                       | string       | None         | yes       |
| masterHost       | Spark master REST server host                              | string       | localhost    | no        |
| masterPort       | Spark master REST server port                              | int          | 6066         | no        |
| secure           | is HTTPS enabled                                           | boolean      | false        | no        |
| submitRequest    | properties required to build Spark job submit request      | map          | false        | yes       |

**Building Spark Job Submit Request**

| KEY                   | DESCRIPTION                                                | TYPE         | DEFAULT      | MANDATORY |
|-----------------------|------------------------------------------------------------|--------------|--------------|-----------|
| appResource           | Spark application resource/ main application jar           | string       | None         | yes       |
| appArgs               | list of application arguments to pass                      | list         | None         | no        |
| environmentVariables  | environment variable to pass to Spark Job                  | map          | None         | no        |
| mainClass             | main application class name                                | string       | None         | yes       |
| sparkProperties       | additional Spark properties to configure                   | map          | None         | no        |

List of available [Spark properties](https://spark.apache.org/docs/latest/configuration.html#viewing-spark-properties) that can be configured.

**Sample**
```
sparkVersion: 2.3.1
masterHost: localhost
masterPort: 6066
clusterMode: spark
secure: true
submitRequest:
  appResource: file:/spark-examples_2.11-2.3.1.jar
  appArgs: []
  environmentVariables:
    SPARK_ENV_LOADED : 1
  mainClass: org.apache.spark.examples.SparkPi
  sparkProperties:
    spark.jars: file:/spark-examples_2.11-2.3.1.jar
    spark.driver.supervise: false
    spark.app.name: MyJob
    spark.eventLog.enabled: false
    spark.submit.deployMode : cluster
```

The above properties can be set at a TaskDefinition level and optionally defining a WorkflowTask. Properties configured at WorkflowTask level takes precedence over TaskDefinition