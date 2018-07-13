package com.cognitree.kronos.scheduler.store;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.WorkflowId;
import com.cognitree.kronos.model.definitions.WorkflowDefinition;
import com.cognitree.kronos.model.definitions.WorkflowDefinition.WorkflowTask;
import com.cognitree.kronos.model.definitions.WorkflowDefinitionId;

import java.util.List;

public class WorkflowStoreService implements StoreService<Workflow, WorkflowId> {

    private final WorkflowStoreConfig workflowStoreConfig;
    private WorkflowStore workflowStore;

    public WorkflowStoreService(WorkflowStoreConfig workflowStoreConfig) {
        this.workflowStoreConfig = workflowStoreConfig;
    }

    public static WorkflowStoreService getService() {
        return (WorkflowStoreService) StoreServiceProvider.getStoreService(WorkflowStoreService.class.getSimpleName());
    }

    @Override
    public void init() throws Exception {
        workflowStore = (WorkflowStore) Class.forName(workflowStoreConfig.getStoreClass())
                .getConstructor().newInstance();
        workflowStore.init(workflowStoreConfig.getConfig());
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        workflowStore.stop();
    }


    @Override
    public void store(Workflow workflow) {
        workflowStore.store(workflow);
    }

    @Override
    public List<Workflow> load() {
        return workflowStore.load();
    }

    @Override
    public Workflow load(WorkflowId workflowId) {
        return workflowStore.load(workflowId);
    }

    public List<Workflow> load(long createdAfter, long createdBefore) {
        return workflowStore.load(createdAfter, createdBefore);
    }

    public List<Workflow> loadByName(String name, String namespace, long createdAfter, long createdBefore) {
        return workflowStore.loadByName(name, namespace, createdAfter, createdBefore);
    }

    public List<Task> getWorkflowTasks(WorkflowId workflowId) {
        return TaskStoreService.getService().loadByWorkflowId(workflowId.getId(), workflowId.getNamespace());
    }

    public List<WorkflowTask> getWorkflowTaskDefs(Workflow workflow) {
        WorkflowDefinitionId workflowDefinitionId =
                WorkflowDefinitionId.create(workflow.getName(), workflow.getNamespace());
        final WorkflowDefinition workflowDefinition =
                WorkflowDefinitionStoreService.getService().load(workflowDefinitionId);
        return workflowDefinition.getTasks();
    }

    @Override
    public void update(Workflow workflow) {
        workflowStore.update(workflow);
    }

    @Override
    public void delete(WorkflowId workflowId) {
        workflowStore.delete(workflowId);
    }
}