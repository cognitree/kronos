package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.TaskStore;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

/**
 * A standard MongoDB based implementation of {@link TaskStore}.
 */
public class MongoTaskStore extends MongoStore implements TaskStore {

    private static final Logger logger = LoggerFactory.getLogger(MongoTaskStore.class);

    private static final String COLLECTION_NAME = "tasks";

    MongoTaskStore(MongoClient mongoClient) {
        super(mongoClient);
    }

    @Override
    public void store(Task task) throws StoreException {
        logger.debug("Received request to store task {}", task);
        insertOne(task.getNamespace(), COLLECTION_NAME, task, Task.class);
    }

    @Override
    public List<Task> load(String namespace) throws StoreException {
        logger.debug("Received request to get all tasks under namespace {}", namespace);
        return findAll(namespace, COLLECTION_NAME, Task.class);
    }

    @Override
    public Task load(TaskId taskId) throws StoreException {
        logger.debug("Received request to load task with id {}", taskId);
        return findOne(taskId.getNamespace(), COLLECTION_NAME,
                and(
                        eq("name", taskId.getName()),
                        eq("job", taskId.getJob()),
                        eq("workflow", taskId.getWorkflow()),
                        eq("namespace", taskId.getNamespace())),
                Task.class);
    }

    @Override
    public List<Task> loadByJobIdAndWorkflowName(String namespace, String jobId, String workflowName) throws StoreException {
        logger.debug("Received request to get all tasks with job id {}, workflow name {}, namespace {}",
                jobId, workflowName, namespace);
        return findMany(namespace, COLLECTION_NAME,
                and(
                        eq("job", jobId),
                        eq("workflow", workflowName),
                        eq("namespace", namespace)),
                Task.class);
    }

    @Override
    public List<Task> loadByStatus(String namespace, List<Task.Status> statuses) throws StoreException {
        logger.debug("Received request to get all tasks with status in {} under namespace {}", statuses, namespace);
        return findMany(namespace, COLLECTION_NAME,
                and(
                        in("status", statuses),
                        eq("namespace", namespace)),
                Task.class);
    }

    @Override
    public Map<Task.Status, Integer> countByStatus(String namespace, long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to count all tasks by status under namespace {}," +
                "created after {}, created before {}", namespace, createdAfter, createdBefore);
        ArrayList<Bson> pipelines = new ArrayList<>();
        pipelines.add(Aggregates.match(
                and(
                        eq("namespace", namespace),
                        lt("createdAt", createdBefore),
                        gt("createdAt", createdAfter))));
        pipelines.add(Aggregates.group("$status", Accumulators.sum("count", 1)));
        return aggregateByStatus(namespace, pipelines);
    }

    @Override
    public Map<Task.Status, Integer> countByStatusForWorkflowName(String namespace, String workflowName,
                                                                  long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to count tasks by status having workflow name {} under namespace {}," +
                "created after {}, created before {}", workflowName, namespace, createdAfter, createdBefore);
        ArrayList<Bson> pipelines = new ArrayList<>();
        pipelines.add(Aggregates.match(
                and(
                        eq("namespace", namespace),
                        eq("workflow", workflowName),
                        lt("createdAt", createdBefore),
                        gt("createdAt", createdAfter))));
        pipelines.add(Aggregates.group("$status", Accumulators.sum("count", 1)));
        return aggregateByStatus(namespace, pipelines);
    }

    private HashMap<Task.Status, Integer> aggregateByStatus(String namespace, ArrayList<Bson> pipelines) throws StoreException {
        ArrayList<Document> aggregates = aggregate(namespace, COLLECTION_NAME, pipelines, Document.class);
        HashMap<Task.Status, Integer> statusMap = new HashMap<>();
        for (Document aggregate : aggregates) {
            Task.Status status = Task.Status.valueOf(aggregate.get("_id").toString());
            statusMap.put(status, aggregate.getInteger("count"));
        }
        return statusMap;
    }

    @Override
    public void update(Task task) throws StoreException {
        logger.debug("Received request to update task to {}", task);
        findOneAndUpdate(task.getNamespace(), COLLECTION_NAME,
                and(
                        eq("name", task.getName()),
                        eq("job", task.getJob()),
                        eq("workflow", task.getWorkflow())),
                combine(
                        set("status", task.getStatus().toString()),
                        set("statusMessage", task.getStatusMessage()),
                        set("submittedAt", task.getSubmittedAt()),
                        set("completedAt", task.getCompletedAt()),
                        set("context", task.getContext())),
                Task.class);
    }

    @Override
    public void delete(TaskId taskId) throws StoreException {
        logger.debug("Received request to delete task with id {}", taskId);
        deleteOne(taskId.getNamespace(), COLLECTION_NAME, eq("job", taskId.getJob()), Task.class);
    }
}
