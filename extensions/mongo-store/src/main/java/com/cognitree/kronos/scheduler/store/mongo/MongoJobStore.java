package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.JobId;
import com.cognitree.kronos.scheduler.store.JobStore;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
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
 * A standard MongoDB based implementation of {@link JobStore}.
 */
public class MongoJobStore implements JobStore {

    private static final Logger logger = LoggerFactory.getLogger(Job.class);

    private static final String COLLECTION_NAME = "jobs";

    private final MongoClient mongoClient;

    MongoJobStore(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Override
    public void store(Job job) throws StoreException {
        logger.debug("Received request to store job {}", job);
        try {
            MongoCollection<Job> jobCollection = getJobCollection(job.getNamespace());
            jobCollection.insertOne(job);
        } catch (Exception e) {
            logger.error("Error storing job {}", job, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Job> load(String namespace) throws StoreException {
        logger.debug("Received request to get all jobs under namespace {}", namespace);
        try {
            MongoCollection<Job> jobCollection = getJobCollection(namespace);
            return jobCollection.find(eq("namespace", namespace)).into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error fetching all jobs under namespace {}", namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Job> load(String namespace, long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to get all jobs under namespace {}, created after {}, created before {}",
                namespace, createdAfter, createdBefore);
        try {
            MongoCollection<Job> jobCollection = getJobCollection(namespace);
            return jobCollection.find(
                    and(
                            eq("namespace", namespace),
                            lt("createdAt", createdBefore),
                            gt("createdAt", createdAfter)))
                    .into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error fetching all jobs under namespace {} created after {}, created before {}",
                    namespace, createdAfter, createdBefore, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Job> loadByWorkflowName(String namespace, String workflowName, long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to get jobs with workflow name {}, namespace {},  created after {}, created before {}",
                workflowName, namespace, createdAfter, createdBefore);
        try {
            MongoCollection<Job> jobCollection = getJobCollection(namespace);
            return jobCollection.find(
                    and(
                            eq("namespace", namespace),
                            eq("workflow", workflowName),
                            lt("createdAt", createdBefore),
                            gt("createdAt", createdAfter)))
                    .into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error fetching jobs with workflow name {}, namespace {}, created after {}, created before {}",
                    workflowName, namespace, createdAfter, createdBefore, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Job> loadByWorkflowNameAndTriggerName(String namespace, String workflowName,
                                                      String triggerName, long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to get all jobs with workflow name {} under namespace {}, triggerName {}," +
                " created after {}, created before {}", workflowName, namespace, triggerName, createdAfter, createdBefore);
        try {
            MongoCollection<Job> jobCollection = getJobCollection(namespace);
            return jobCollection.find(
                    and(
                            eq("namespace", namespace),
                            eq("workflow", workflowName),
                            eq("trigger", triggerName),
                            lt("createdAt", createdBefore),
                            gt("createdAt", createdAfter)))
                    .into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error fetching all jobs with workflow name {} under namespace {} created after {}, " +
                    "created before {}", workflowName, namespace, createdAfter, createdBefore, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Job> loadByStatus(String namespace, List<Job.Status> statuses,
                                  long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to get jobs with status in {} under namespace {}, created after {}, created before {}",
                statuses, namespace, createdAfter, createdBefore);
        try {
            MongoCollection<Job> jobCollection = getJobCollection(namespace);
            return jobCollection.find(
                    and(
                            eq("namespace", namespace),
                            in("status", statuses),
                            lt("createdAt", createdBefore),
                            gt("createdAt", createdAfter)))
                    .into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error fetching jobs with status in {} under namespace {}, created after {}",
                    statuses, namespace, createdAfter, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Job> loadByWorkflowNameAndStatus(String namespace, String workflowName,
                                                 List<Job.Status> statuses, long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to get jobs having workflow name {} with status in {} under namespace {}, " +
                " created after {}, created before {}", workflowName, statuses, namespace, createdAfter, createdBefore);
        try {
            MongoCollection<Job> jobCollection = getJobCollection(namespace);
            return jobCollection.find(
                    and(
                            eq("namespace", namespace),
                            eq("workflow", workflowName),
                            in("status", statuses),
                            lt("createdAt", createdBefore),
                            gt("createdAt", createdAfter)))
                    .into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error fetching jobs having workflow name {} with status in {} under namespace {}, " +
                    "created after {}, created before {}", workflowName, statuses, namespace, createdAfter, createdBefore, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public List<Job> loadByWorkflowNameAndTriggerNameAndStatus(String namespace, String workflowName,
                                                               String triggerName, List<Job.Status> statuses,
                                                               long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to get jobs having workflow name {}, trigger name {} with status in {} " +
                        "under namespace {}, created after {}, created before {}",
                workflowName, triggerName, statuses, namespace, createdAfter, createdBefore);
        try {
            MongoCollection<Job> jobCollection = getJobCollection(namespace);
            return jobCollection.find(
                    and(
                            eq("namespace", namespace),
                            eq("workflow", workflowName),
                            eq("trigger", triggerName),
                            in("status", statuses),
                            lt("createdAt", createdBefore),
                            gt("createdAt", createdAfter)))
                    .into(new ArrayList<>());
        } catch (Exception e) {
            logger.error("Error fetching jobs having workflow name {}, trigger name {} with status in {} under " +
                            "namespace {}, created after {}, created before {}",
                    workflowName, triggerName, statuses, namespace, createdAfter, createdBefore, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public Map<Job.Status, Integer> countByStatus(String namespace, long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to count jobs by status under namespace {}, created after {}, created before {}",
                namespace, createdAfter, createdBefore);
        try {
            MongoCollection<Document> jobCollection = mongoClient.getDatabase(namespace)
                    .getCollection(COLLECTION_NAME);
            Bson filter = Aggregates.match(and(
                    eq("namespace", namespace),
                    lt("createdAt", createdBefore),
                    gt("createdAt", createdAfter)));
            return aggregateByStatus(jobCollection, filter);
        } catch (Exception e) {
            logger.error("Error counting jobs by status under namespace {}, created after {}, created before {}",
                    namespace, createdAfter, createdBefore, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public Map<Job.Status, Integer> countByStatusForWorkflowName(String namespace, String workflowName,
                                                                 long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to count by status having workflow name {}, namespace {}, created after {}, created before {}",
                workflowName, namespace, createdAfter, createdBefore);
        try {
            MongoCollection<Document> jobCollection = mongoClient.getDatabase(namespace)
                    .getCollection(COLLECTION_NAME);
            Bson filter = Aggregates.match(and(
                    eq("namespace", namespace),
                    eq("workflow", workflowName),
                    lt("createdAt", createdBefore),
                    gt("createdAt", createdAfter)));
            return aggregateByStatus(jobCollection, filter);
        } catch (Exception e) {
            logger.error("Error counting jobs by status having workflow name {} under namespace {}, created after {}, created before {}",
                    workflowName, namespace, createdAfter, createdBefore, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    private HashMap<Job.Status, Integer> aggregateByStatus(MongoCollection<Document> collection, Bson filter) {
        ArrayList<Bson> pipelines = new ArrayList<>();
        pipelines.add(filter);
        pipelines.add(Aggregates.group("$status", Accumulators.sum("count", 1)));
        AggregateIterable<Document> aggregates = collection.aggregate(pipelines);
        HashMap<Job.Status, Integer> statusMap = new HashMap<>();
        for (Document aggregate : aggregates) {
            Job.Status status = Job.Status.valueOf(aggregate.get("_id").toString());
            statusMap.put(status, aggregate.getInteger("count"));
        }
        return statusMap;
    }

    @Override
    public Job load(JobId jobId) throws StoreException {
        logger.debug("Received request to delete job with id {}", jobId);
        try {
            MongoCollection<Job> jobCollection = getJobCollection(jobId.getNamespace());
            return jobCollection.find(eq("_id", jobId.getId())).first();
        } catch (Exception e) {
            logger.error("Error deleting job with id {}", jobId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void update(Job job) throws StoreException {
        logger.info("Received request to update job to {}", job);
        try {
            MongoCollection<Job> jobCollection = getJobCollection(job.getNamespace());
            jobCollection.findOneAndUpdate(
                    eq("_id", job.getId()),
                    combine(
                            set("status", job.getStatus().toString()),
                            set("createdAt", job.getCreatedAt()),
                            set("completedAt", job.getCompletedAt())));
        } catch (Exception e) {
            logger.error("Error updating job to {}", job, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void delete(JobId jobId) throws StoreException {
        logger.debug("Received request to delete job with id {}", jobId);
        try {
            MongoCollection<Job> jobCollection = getJobCollection(jobId.getNamespace());
            jobCollection.deleteOne(
                    and(
                            eq("_id", jobId.getId()),
                            eq("workflow", jobId.getWorkflow()),
                            eq("namespace", jobId.getNamespace())));
        } catch (Exception e) {
            logger.error("Error deleting job with id {}", jobId, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public void deleteByWorkflowName(String namespace, String workflowName) throws StoreException {
        logger.debug("Received request to delete jobs with workflow name {}, namespace {}",
                workflowName, namespace);
        try {
            MongoCollection<Job> jobCollection = getJobCollection(namespace);
            jobCollection.deleteOne(
                    and(
                            eq("namespace", namespace),
                            eq("workflow", workflowName)));
        } catch (Exception e) {
            logger.error("Error deleting jobs with workflow name {}, namespace {}", workflowName, namespace, e);
            throw new StoreException(e.getMessage(), e.getCause());
        }
    }

    private MongoCollection<Job> getJobCollection(String namespace) {
        return mongoClient.getDatabase(namespace).getCollection(COLLECTION_NAME, Job.class);
    }
}
