package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import com.mongodb.client.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

/**
 * A standard MongoDB based implementation of {@link WorkflowStore}.
 */
public class MongoWorkflowStore extends MongoStore<Workflow> implements WorkflowStore {

    private static final Logger logger = LoggerFactory.getLogger(MongoWorkflowStore.class);

    private static final String COLLECTION_NAME = "workflows";

    MongoWorkflowStore(MongoClient mongoClient) {
        super(mongoClient, Workflow.class);
    }

    @Override
    public void store(Workflow workflow) throws StoreException {
        logger.debug("Received request to store workflow {}", workflow);
        insertOne(workflow.getNamespace(), COLLECTION_NAME, workflow);
    }

    @Override
    public List<Workflow> load(String namespace) throws RuntimeException, StoreException {
        logger.debug("Received request to get all workflow under namespace {}", namespace);
        return findMany(namespace, COLLECTION_NAME, eq("namespace", namespace));
    }

    @Override
    public Workflow load(WorkflowId workflowId) throws StoreException {
        logger.debug("Received request to load workflow with id {}", workflowId);
        return findOne(workflowId.getNamespace(), COLLECTION_NAME,
                and(
                        eq("name", workflowId.getName()),
                        eq("namespace", workflowId.getNamespace())));
    }

    @Override
    public void update(Workflow workflow) throws StoreException {
        logger.debug("Received request to update workflow to {}", workflow);
        findOneAndUpdate(workflow.getNamespace(), COLLECTION_NAME,
                eq("name", workflow.getName()),
                combine(
                        set("description", workflow.getDescription()),
                        set("tasks", workflow.getTasks())));
    }

    @Override
    public void delete(WorkflowId workflowId) throws StoreException {
        logger.debug("Received request to delete workflow with id {}", workflowId);
        deleteOne(workflowId.getNamespace(), COLLECTION_NAME,
                and(
                        eq("name", workflowId.getName()),
                        eq("namespace", workflowId.getNamespace())));
    }
}
