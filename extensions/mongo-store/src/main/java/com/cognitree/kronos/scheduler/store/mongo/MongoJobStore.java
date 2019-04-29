/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognitree.kronos.scheduler.store.mongo;

import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.JobId;
import com.cognitree.kronos.scheduler.store.JobStore;
import com.cognitree.kronos.scheduler.store.StoreException;
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
 * A standard MongoDB based implementation of {@link JobStore}.
 */
public class MongoJobStore extends MongoStore<Job> implements JobStore {

    private static final Logger logger = LoggerFactory.getLogger(MongoJobStore.class);

    private static final String COLLECTION_NAME = "jobs";

    MongoJobStore(MongoClient mongoClient) {
        super(mongoClient, Job.class);
    }

    @Override
    public void store(Job job) throws StoreException {
        logger.debug("Received request to store job {}", job);
        insertOne(job.getNamespace(), COLLECTION_NAME, job);
    }

    @Override
    public List<Job> load(String namespace) throws StoreException {
        logger.debug("Received request to get all jobs under namespace {}", namespace);
        return findMany(namespace, COLLECTION_NAME, eq("namespace", namespace));
    }

    @Override
    public List<Job> load(String namespace, long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to get all jobs under namespace {}, created after {}, created before {}",
                namespace, createdAfter, createdBefore);
        return findMany(namespace, COLLECTION_NAME,
                and(
                        eq("namespace", namespace),
                        lt("createdAt", createdBefore),
                        gt("createdAt", createdAfter)));
    }

    @Override
    public List<Job> loadByWorkflowName(String namespace, String workflowName, long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to get jobs with workflow name {}, namespace {},  created after {}, created before {}",
                workflowName, namespace, createdAfter, createdBefore);
        return findMany(namespace, COLLECTION_NAME,
                and(
                        eq("namespace", namespace),
                        eq("workflow", workflowName),
                        lt("createdAt", createdBefore),
                        gt("createdAt", createdAfter)));
    }

    @Override
    public List<Job> loadByWorkflowNameAndTriggerName(String namespace, String workflowName,
                                                      String triggerName, long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to get all jobs with workflow name {} under namespace {}, triggerName {}," +
                " created after {}, created before {}", workflowName, namespace, triggerName, createdAfter, createdBefore);
        return findMany(namespace, COLLECTION_NAME,
                and(
                        eq("namespace", namespace),
                        eq("workflow", workflowName),
                        eq("trigger", triggerName),
                        lt("createdAt", createdBefore),
                        gt("createdAt", createdAfter)));
    }

    @Override
    public List<Job> loadByStatus(String namespace, List<Job.Status> statuses,
                                  long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to get jobs with status in {} under namespace {}, created after {}, created before {}",
                statuses, namespace, createdAfter, createdBefore);
        return findMany(namespace, COLLECTION_NAME,
                and(
                        eq("namespace", namespace),
                        in("status", statuses),
                        lt("createdAt", createdBefore),
                        gt("createdAt", createdAfter)));
    }

    @Override
    public List<Job> loadByWorkflowNameAndStatus(String namespace, String workflowName,
                                                 List<Job.Status> statuses, long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to get jobs having workflow name {} with status in {} under namespace {}, " +
                " created after {}, created before {}", workflowName, statuses, namespace, createdAfter, createdBefore);
        return findMany(namespace, COLLECTION_NAME,
                and(
                        eq("namespace", namespace),
                        eq("workflow", workflowName),
                        in("status", statuses),
                        lt("createdAt", createdBefore),
                        gt("createdAt", createdAfter)));
    }

    @Override
    public List<Job> loadByWorkflowNameAndTriggerNameAndStatus(String namespace, String workflowName,
                                                               String triggerName, List<Job.Status> statuses,
                                                               long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to get jobs having workflow name {}, trigger name {} with status in {} " +
                        "under namespace {}, created after {}, created before {}",
                workflowName, triggerName, statuses, namespace, createdAfter, createdBefore);
        return findMany(namespace, COLLECTION_NAME,
                and(
                        eq("namespace", namespace),
                        eq("workflow", workflowName),
                        eq("trigger", triggerName),
                        in("status", statuses),
                        lt("createdAt", createdBefore),
                        gt("createdAt", createdAfter)));

    }

    @Override
    public Map<Job.Status, Integer> countByStatus(String namespace, long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to count jobs by status under namespace {}, created after {}, created before {}",
                namespace, createdAfter, createdBefore);
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
    public Map<Job.Status, Integer> countByStatusForWorkflowName(String namespace, String workflowName,
                                                                 long createdAfter, long createdBefore) throws StoreException {
        logger.debug("Received request to count by status having workflow name {}, namespace {}, created after {}, created before {}",
                workflowName, namespace, createdAfter, createdBefore);
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

    private Map<Job.Status, Integer> aggregateByStatus(String namespace, ArrayList<Bson> pipelines) throws StoreException {
        ArrayList<Document> aggregates = aggregate(namespace, COLLECTION_NAME, pipelines);
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
        return findOne(jobId.getNamespace(), COLLECTION_NAME, eq("_id", jobId.getId()));
    }

    @Override
    public void update(Job job) throws StoreException {
        logger.info("Received request to update job to {}", job);
        findOneAndUpdate(job.getNamespace(), COLLECTION_NAME,
                eq("_id", job.getId()),
                combine(
                        set("status", job.getStatus().toString()),
                        set("createdAt", job.getCreatedAt()),
                        set("completedAt", job.getCompletedAt())));
    }

    @Override
    public void delete(JobId jobId) throws StoreException {
        logger.debug("Received request to delete job with id {}", jobId);
        deleteOne(jobId.getNamespace(), COLLECTION_NAME,
                and(
                        eq("_id", jobId.getId()),
                        eq("workflow", jobId.getWorkflow()),
                        eq("namespace", jobId.getNamespace())));
    }

    @Override
    public void deleteByWorkflowName(String namespace, String workflowName) throws StoreException {
        logger.debug("Received request to delete jobs with workflow name {}, namespace {}",
                workflowName, namespace);
        deleteOne(namespace, COLLECTION_NAME,
                and(
                        eq("namespace", namespace),
                        eq("workflow", workflowName)));
    }
}
