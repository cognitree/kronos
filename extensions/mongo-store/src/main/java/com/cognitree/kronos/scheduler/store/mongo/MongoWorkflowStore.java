package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

/**
 * A standard MongoDB based implementation of {@link WorkflowStore}.
 */
public class MongoWorkflowStore implements WorkflowStore {

    private static final Logger logger = LoggerFactory.getLogger(MongoWorkflowStore.class);

    private static final String COLLECTION_NAME = "workflows";

    private final MongoClient mongoClient;

    MongoWorkflowStore(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Override
    public void store(Workflow workflow) throws StoreException {
        logger.debug("Received request to store workflow {}", workflow);
        try {
            MongoCollection<Workflow> workflowCollection = getWorkflowCollection(workflow.getNamespace());
            workflowCollection.insertOne(workflow);
        } catch (Exception e) {
            logger.error("Error storing workflow {}", workflow, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Workflow> load(String namespace) throws RuntimeException, StoreException {
        logger.debug("Received request to get all workflow under namespace {}", namespace);
        try {
            MongoCollection<Workflow> workflowCollection = getWorkflowCollection(namespace);
            return workflowCollection.find(eq("namespace", namespace)).into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error fetching all workflow under namespace {}", namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public Workflow load(WorkflowId workflowId) throws StoreException {
        logger.debug("Received request to load workflow with id {}", workflowId);
        try {
            MongoCollection<Workflow> workflowCollection = getWorkflowCollection(workflowId.getNamespace());
            return workflowCollection.find(
                    and(
                            eq("name", workflowId.getName()),
                            eq("namespace", workflowId.getNamespace())))
                    .first();
        } catch (Exception e) {
            logger.error("Error fetching workflow with id {}", workflowId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void update(Workflow workflow) throws StoreException {
        logger.debug("Received request to update workflow to {}", workflow);
        try {
            MongoCollection<Workflow> workflowCollection = getWorkflowCollection(workflow.getNamespace());
            workflowCollection.findOneAndUpdate(
                    eq("name", workflow.getName()),
                    combine(
                            set("description", workflow.getDescription()),
                            set("tasks", workflow.getTasks())));
        } catch (Exception e) {
            logger.error("Error updating workflow to {}", workflow, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void delete(WorkflowId workflowId) throws StoreException {
        logger.debug("Received request to delete workflow with id {}", workflowId);
        try {
            MongoCollection<Workflow> workflowCollection = getWorkflowCollection(workflowId.getNamespace());
            workflowCollection.deleteOne(
                    and(
                            eq("name", workflowId.getName()),
                            eq("namespace", workflowId.getNamespace())));
        } catch (Exception e) {
            logger.error("Error deleting workflow with id {}", workflowId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    private MongoCollection<Workflow> getWorkflowCollection(String namespace) {
        return mongoClient.getDatabase(namespace)
                .getCollection(COLLECTION_NAME, Workflow.class);
    }
}
