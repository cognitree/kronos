# Shell Command Handler

Shell command handler is an extension for task handler which executes the configured shell command.

## Configuring Shell Command Handler

Update the `taskHandlerConfig` section in `executor.yaml` to configure to shell command handler.

```
taskHandlerConfig:
  # type of task to be handled by handler
  shellCommand:
    # handler implementation used to handle task of type shellCommand
    handlerClass: com.cognitree.kronos.executor.handlers.ShellCommandHandler
    # max parallel tasks handler is allowed to execute at any point of time
    maxParallelTasks: 4
```

Here, A [ShellCommandHandler](src/main/java/com/cognitree/kronos/executor/handlers/ShellCommandHandler.java) is configured for task type `shellCommand`. Tasks of type `shellCommand` will be executed by shell command handler.

## Configurable Task Properties

A number of properties can be passed along with the task to the handler which is used to execute the task.

Configurable properties supported by Shell Command Handler are as below:

| KEY              | DESCRIPTION                                                | TYPE         | DEFAULT      | MANDATORY |
|------------------|------------------------------------------------------------|--------------|--------------|-----------|
| workingDir       | working dir to set before executing shell command          | string       | None         | yes       |
| logDir           | log dir to use to store stdout and stderr                  | string       | None         | yes       |
| cmd              | shell command to execute                                   | string       | None         | yes       |
| args             | arguments to pass to shell command                         | string       | None         | no        |

**Sample**
```
cmd: echo
args: Hello World from task one
workingDir: /home
logDir: /tmp
```

The above properties can be set at a TaskDefinition level and optionally while defining a WorkflowTask. Properties configured at WorkflowTask level takes precedence over TaskDefinition
