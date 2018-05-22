# Shell Command Handler

Shell command handler is an extension for task handler which executes the configured shell command.

## Configuring Shell Command Handler

Update the `handlerConfig` section in `app.yaml` to configure to shell command handler.

```
handlerConfig:
  # type of task to be handled by handler
  shellCommand:
    handlerClass: com.cognitree.kronos.executor.handlers.ShellCommandHandler
    maxExecutionTime: 10m
```

Here, A [ShellCommandHandler](src/main/java/com/cognitree/kronos/executor/handlers/ShellCommandHandler.java) is configured for task type `shellCommand`. Task with `shellCommand` will be executed by shell command handler.