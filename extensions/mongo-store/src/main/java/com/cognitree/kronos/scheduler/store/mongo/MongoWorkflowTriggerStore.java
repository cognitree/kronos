package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.model.WorkflowTriggerId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.WorkflowTriggerStore;
import com.mongodb.BasicDBObject;
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
 * A standard MongoDB based implementation of {@link WorkflowTrigger}.
 */
public class MongoWorkflowTriggerStore implements WorkflowTriggerStore {

    private static final Logger logger = LoggerFactory.getLogger(MongoWorkflowTriggerStore.class);

    private static final String COLLECTION_NAME = "workflow_triggers";

    private final MongoClient mongoClient;

    MongoWorkflowTriggerStore(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Override
    public void store(WorkflowTrigger workflowTrigger) throws StoreException {
        logger.debug("Received request to store workflow trigger {}", workflowTrigger);
        try {
            MongoCollection<WorkflowTrigger> mongoCollection = getMongoCollection(workflowTrigger.getNamespace());
            mongoCollection.insertOne(workflowTrigger);
        } catch (Exception e) {
            logger.error("Error storing workflow trigger {}", workflowTrigger, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<WorkflowTrigger> load(String namespace) throws StoreException {
        logger.debug("Received request to get all workflow triggers under namespace {}", namespace);
        try {
            MongoCollection<WorkflowTrigger> mongoCollection = getMongoCollection(namespace);
            return mongoCollection.find(eq("namespace", namespace)).into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error fetching workflow triggers under namespace {}", namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<WorkflowTrigger> loadByWorkflowName(String namespace, String workflowName) throws StoreException {
        logger.debug("Received request to get all workflow triggers with workflow name {} under namespace {}",
                workflowName, namespace);
        try {
            MongoCollection<WorkflowTrigger> mongoCollection = getMongoCollection(namespace);
            BasicDBObject query = new BasicDBObject("namespace", namespace)
                    .append("workflow", workflowName);
            return mongoCollection.find(
                    and(
                            eq("workflow", workflowName),
                            eq("namespace", namespace)))
                    .into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error fetching workflow triggers with workflow name {} under namespace {}",
                    workflowName, namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<WorkflowTrigger> loadByWorkflowNameAndEnabled(String namespace,
                                                              String workflowName, boolean enabled) throws StoreException {
        logger.debug("Received request to get all enabled {} workflow triggers with workflow name {} under namespace {}",
                enabled, workflowName, namespace);
        try {
            MongoCollection<WorkflowTrigger> mongoCollection = getMongoCollection(namespace);
            return mongoCollection.find(
                    and(
                            eq("workflow", workflowName),
                            eq("enabled", enabled),
                            eq("namespace", namespace)))
                    .into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error fetching all enabled {} workflow triggers with workflow name {} under namespace {}",
                    enabled, workflowName, namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public WorkflowTrigger load(WorkflowTriggerId workflowTriggerId) throws StoreException {
        logger.debug("Received request to load workflow trigger with id {}", workflowTriggerId);
        try {
            MongoCollection<WorkflowTrigger> mongoCollection = getMongoCollection(workflowTriggerId.getNamespace());
            return mongoCollection.find(
                    and(
                            eq("name", workflowTriggerId.getName()),
                            eq("workflow", workflowTriggerId.getWorkflow()),
                            eq("namespace", workflowTriggerId.getNamespace())))
                    .first();
        } catch (Exception e) {
            logger.error("Error fetching workflow trigger with id {}", workflowTriggerId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void update(WorkflowTrigger workflowTrigger) throws StoreException {
        logger.debug("Received request to update workflow trigger to {}", workflowTrigger);
        try {
            MongoCollection<WorkflowTrigger> mongoCollection = getMongoCollection(workflowTrigger.getNamespace());
            mongoCollection.findOneAndUpdate(
                    and(
                            eq("name", workflowTrigger.getName()),
                            eq("workflow", workflowTrigger.getWorkflow())),
                    combine(
                            set("startAt", workflowTrigger.getStartAt()),
                            set("schedule", workflowTrigger.getSchedule()),
                            set("endAt", workflowTrigger.getEndAt()),
                            set("enabled", workflowTrigger.isEnabled()))
            );
        } catch (Exception e) {
            logger.error("Error updating workflow trigger {}", workflowTrigger, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void delete(WorkflowTriggerId workflowTriggerId) throws StoreException {
        logger.debug("Received request to delete workflow trigger with id {}", workflowTriggerId);
        try {
            MongoCollection<WorkflowTrigger> mongoCollection = getMongoCollection(workflowTriggerId.getNamespace());
            mongoCollection.deleteOne(
                    and(
                            eq("name", workflowTriggerId.getName()),
                            eq("workflow", workflowTriggerId.getWorkflow()),
                            eq("namespace", workflowTriggerId.getNamespace())));
        } catch (Exception e) {
            logger.error("Error delete workflow trigger with id {}", workflowTriggerId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    private MongoCollection<WorkflowTrigger> getMongoCollection(String namespace) {
        return mongoClient.getDatabase(namespace).getCollection(COLLECTION_NAME, WorkflowTrigger.class);
    }
}
