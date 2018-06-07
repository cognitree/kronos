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

Here, A [ShellCommandHandler](src/main/java/com/cognitree/kronos/executor/handlers/ShellCommandHandler.java) is configured for task type `shellCommand`. Task with `shellCommand` will be executed by shell command handler.