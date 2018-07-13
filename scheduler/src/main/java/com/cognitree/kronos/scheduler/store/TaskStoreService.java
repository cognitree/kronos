package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskId;

import java.util.List;

public class TaskStoreService implements StoreService<Task, TaskId> {

    private TaskStoreConfig taskStoreConfig;
    private TaskStore taskStore;

    public TaskStoreService(TaskStoreConfig taskStoreConfig) {
        this.taskStoreConfig = taskStoreConfig;
    }

    public static TaskStoreService getService() {
        return (TaskStoreService) StoreServiceProvider.getStoreService(TaskStoreService.class.getSimpleName());
    }

    public void init() throws Exception {
        taskStore = (TaskStore) Class.forName(taskStoreConfig.getStoreClass())
                .getConstructor().newInstance();
        taskStore.init(taskStoreConfig.getConfig());
    }

    public void start() {

    }

    public void stop() {
        taskStore.stop();
    }

    public List<Task> load() {
        return taskStore.load();
    }

    public Task load(TaskId taskId) {
        return taskStore.load(taskId);
    }

    public List<Task> loadByNameAndWorkflowId(String taskName, String workflowId) {
        return taskStore.loadByNameAndWorkflowId(taskName, workflowId);
    }

    public List<Task> loadByWorkflowId(String workflowId, String namespace) {
        return taskStore.loadByWorkflowId(workflowId, namespace);
    }

    public List<Task> load(List<Task.Status> statuses) {
        return taskStore.load(statuses);
    }

    public void store(Task taskDefinition) {
        taskStore.store(taskDefinition);
    }

    public void update(Task task) {
        taskStore.update(task);
    }

    public void delete(TaskId taskId) {
        taskStore.delete(taskId);
    }
}
