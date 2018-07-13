package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.model.definitions.TaskDefinition;
import com.cognitree.kronos.model.definitions.TaskDefinitionId;

import java.util.List;

public class TaskDefinitionStoreService implements StoreService<TaskDefinition, TaskDefinitionId> {

    private final TaskDefinitionStoreConfig taskDefinitionStoreConfig;
    private TaskDefinitionStore taskDefinitionStore;

    public TaskDefinitionStoreService(TaskDefinitionStoreConfig taskDefinitionStoreConfig) {
        this.taskDefinitionStoreConfig = taskDefinitionStoreConfig;
    }

    public static TaskDefinitionStoreService getService() {
        return (TaskDefinitionStoreService) StoreServiceProvider.getStoreService(TaskDefinitionStoreService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        taskDefinitionStore = (TaskDefinitionStore) Class.forName(taskDefinitionStoreConfig.getStoreClass())
                .getConstructor().newInstance();
        taskDefinitionStore.init(taskDefinitionStoreConfig.getConfig());
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        taskDefinitionStore.stop();
    }

    public List<TaskDefinition> load() {
        return taskDefinitionStore.load();
    }

    public TaskDefinition load(TaskDefinitionId taskDefinitionId) {
        return taskDefinitionStore.load(taskDefinitionId);
    }

    public void store(TaskDefinition taskDefinition) {
        taskDefinitionStore.store(taskDefinition);
    }

    public void update(TaskDefinition taskDefinition) {
        taskDefinitionStore.store(taskDefinition);
    }

    public void delete(TaskDefinitionId taskDefinitionId) {
        taskDefinitionStore.delete(taskDefinitionId);
    }

}
