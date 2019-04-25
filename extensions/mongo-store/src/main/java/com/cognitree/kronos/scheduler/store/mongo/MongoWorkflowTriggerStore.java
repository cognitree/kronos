package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.model.WorkflowTriggerId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;
import com.mongodb.client.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

/**
 * A standard MongoDB based implementation of {@link WorkflowTrigger}.
 */
public class MongoWorkflowTriggerStore extends MongoStore<WorkflowTrigger> implements WorkflowTriggerStore {

    private static final Logger logger = LoggerFactory.getLogger(MongoWorkflowTriggerStore.class);

    private static final String COLLECTION_NAME = "workflow_triggers";

    MongoWorkflowTriggerStore(MongoClient mongoClient) {
        super(mongoClient, WorkflowTrigger.class);
    }

    @Override
    public void store(WorkflowTrigger workflowTrigger) throws StoreException {
        logger.debug("Received request to store workflow trigger {}", workflowTrigger);
        insertOne(workflowTrigger.getNamespace(), COLLECTION_NAME, workflowTrigger);
    }

    @Override
    public List<WorkflowTrigger> load(String namespace) throws StoreException {
        logger.debug("Received request to get all workflow triggers under namespace {}", namespace);
        return findMany(namespace, COLLECTION_NAME, eq("namespace", namespace));
    }

    @Override
    public List<WorkflowTrigger> loadByWorkflowName(String namespace, String workflowName) throws StoreException {
        logger.debug("Received request to get all workflow triggers with workflow name {} under namespace {}",
                workflowName, namespace);
        return findMany(namespace, COLLECTION_NAME,
                and(
                        eq("workflow", workflowName),
                        eq("namespace", namespace)));
    }

    @Override
    public List<WorkflowTrigger> loadByWorkflowNameAndEnabled(String namespace, String workflowName, boolean enabled)
            throws StoreException {
        logger.debug("Received request to get all enabled {} workflow triggers with workflow name {} under namespace {}",
                enabled, workflowName, namespace);
        return findMany(namespace, COLLECTION_NAME,
                and(
                        eq("workflow", workflowName),
                        eq("enabled", enabled),
                        eq("namespace", namespace)));
    }

    @Override
    public WorkflowTrigger load(WorkflowTriggerId workflowTriggerId) throws StoreException {
        logger.debug("Received request to load workflow trigger with id {}", workflowTriggerId);
        return findOne(workflowTriggerId.getNamespace(), COLLECTION_NAME,
                and(
                        eq("name", workflowTriggerId.getName()),
                        eq("workflow", workflowTriggerId.getWorkflow()),
                        eq("namespace", workflowTriggerId.getNamespace())));
    }

    @Override
    public void update(WorkflowTrigger workflowTrigger) throws StoreException {
        logger.debug("Received request to update workflow trigger to {}", workflowTrigger);
        findOneAndUpdate(workflowTrigger.getNamespace(), COLLECTION_NAME,
                and(
                        eq("name", workflowTrigger.getName()),
                        eq("workflow", workflowTrigger.getWorkflow())),
                combine(
                        set("startAt", workflowTrigger.getStartAt()),
                        set("schedule", workflowTrigger.getSchedule()),
                        set("endAt", workflowTrigger.getEndAt()),
                        set("enabled", workflowTrigger.isEnabled()),
                        set("properties", workflowTrigger.getProperties())));
    }

    @Override
    public void delete(WorkflowTriggerId workflowTriggerId) throws StoreException {
        logger.debug("Received request to delete workflow trigger with id {}", workflowTriggerId);
        deleteOne(workflowTriggerId.getNamespace(), COLLECTION_NAME,
                and(
                        eq("name", workflowTriggerId.getName()),
                        eq("workflow", workflowTriggerId.getWorkflow()),
                        eq("namespace", workflowTriggerId.getNamespace())));
    }
}
