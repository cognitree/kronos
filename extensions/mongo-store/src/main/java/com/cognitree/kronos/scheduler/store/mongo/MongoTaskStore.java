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
public class MongoTaskStore extends MongoStore<Task> implements TaskStore {

    private static final Logger logger = LoggerFactory.getLogger(MongoTaskStore.class);

    private static final String COLLECTION_NAME = "tasks";

    MongoTaskStore(MongoClient mongoClient) {
        super(mongoClient, Task.class);
    }

    @Override
    public void store(Task task) throws StoreException {
        logger.debug("Received request to store task {}", task);
        insertOne(task.getNamespace(), COLLECTION_NAME, task);
    }

    @Override
    public List<Task> load(String namespace) throws StoreException {
        logger.debug("Received request to get all tasks under namespace {}", namespace);
        return findAll(namespace, COLLECTION_NAME);
    }

    @Override
    public Task load(TaskId taskId) throws StoreException {
        logger.debug("Received request to load task with id {}", taskId);
        return findOne(taskId.getNamespace(), COLLECTION_NAME,
                and(
                        eq("name", taskId.getName()),
                        eq("job", taskId.getJob()),
                        eq("workflow", taskId.getWorkflow()),
                        eq("namespace", taskId.getNamespace())));
    }

    @Override
    public List<Task> loadByJobIdAndWorkflowName(String namespace, String jobId, String workflowName) throws StoreException {
        logger.debug("Received request to get all tasks with job id {}, workflow name {}, namespace {}",
                jobId, workflowName, namespace);
        return findMany(namespace, COLLECTION_NAME,
                and(
                        eq("job", jobId),
                        eq("workflow", workflowName),
                        eq("namespace", namespace)));
    }

    @Override
    public List<Task> loadByStatus(String namespace, List<Task.Status> statuses) throws StoreException {
        logger.debug("Received request to get all tasks with status in {} under namespace {}", statuses, namespace);
        return findMany(namespace, COLLECTION_NAME,
                and(
                        in("status", statuses),
                        eq("namespace", namespace)));
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
        ArrayList<Document> aggregates = aggregate(namespace, COLLECTION_NAME, pipelines);
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
                        set("context", task.getContext())));
    }

    @Override
    public void delete(TaskId taskId) throws StoreException {
        logger.debug("Received request to delete task with id {}", taskId);
        deleteOne(taskId.getNamespace(), COLLECTION_NAME, eq("job", taskId.getJob()));
    }
}
