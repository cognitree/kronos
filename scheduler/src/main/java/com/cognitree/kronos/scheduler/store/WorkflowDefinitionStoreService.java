package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.ReviewPending;
import com.cognitree.kronos.model.definitions.WorkflowDefinition;
import com.cognitree.kronos.model.definitions.WorkflowDefinitionId;

import java.util.List;

@ReviewPending
public class WorkflowDefinitionStoreService implements StoreService<WorkflowDefinition, WorkflowDefinitionId> {

    private final WorkflowDefinitionStoreConfig workflowDefinitionStoreConfig;
    private WorkflowDefinitionStore workflowDefinitionStore;

    public WorkflowDefinitionStoreService(WorkflowDefinitionStoreConfig workflowDefinitionStoreConfig) {
        this.workflowDefinitionStoreConfig = workflowDefinitionStoreConfig;
    }

    public static WorkflowDefinitionStoreService getService() {
        return (WorkflowDefinitionStoreService) StoreServiceProvider.getStoreService(WorkflowDefinitionStoreService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        workflowDefinitionStore = (WorkflowDefinitionStore) Class.forName(workflowDefinitionStoreConfig.getStoreClass())
                .getConstructor().newInstance();
        workflowDefinitionStore.init(workflowDefinitionStoreConfig.getConfig());
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        workflowDefinitionStore.stop();
    }

    public void store(WorkflowDefinition workflowDefinition) {
        workflowDefinitionStore.store(workflowDefinition);
    }

    public List<WorkflowDefinition> load() {
        return workflowDefinitionStore.load();
    }

    public WorkflowDefinition load(WorkflowDefinitionId workflowDefinitionId) {
        return workflowDefinitionStore.load(workflowDefinitionId);
    }

    public void update(WorkflowDefinition workflowDefinition) {
        workflowDefinitionStore.store(workflowDefinition);
    }

    public void delete(WorkflowDefinitionId workflowDefinitionId) {
        workflowDefinitionStore.delete(workflowDefinitionId);
    }
}
