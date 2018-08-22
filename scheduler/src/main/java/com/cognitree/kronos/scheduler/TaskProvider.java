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

package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.scheduler.store.TaskStoreService;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.cognitree.kronos.model.Task.Status.CREATED;
import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.model.Task.Status.RUNNING;
import static com.cognitree.kronos.model.Task.Status.SCHEDULED;
import static com.cognitree.kronos.model.Task.Status.SUBMITTED;
import static com.cognitree.kronos.model.Task.Status.SUCCESSFUL;
import static com.cognitree.kronos.model.Task.Status.WAITING;
import static com.cognitree.kronos.util.DateTimeUtil.resolveDuration;

/**
 * Task provider manages/ resolves task dependencies and exposes APIs to add, remove, retrieve tasks in active and
 * ready-to-execute state.
 * <p>
 * Internally, task provider is backed by a directed acyclic graph and persistence store to manage dependencies
 * across these tasks.
 */
final class TaskProvider {
    private static final Logger logger = LoggerFactory.getLogger(TaskProvider.class);

    private final MutableGraph<Task> graph = GraphBuilder.directed().build();

    void init() {
        logger.info("Initializing task provider from task store");
        // TODO fix : load tasks from all namespaces
        final List<Task> tasks = TaskStoreService.getService()
                .load(Arrays.asList(CREATED, WAITING, SCHEDULED, SUBMITTED, RUNNING), null);
        if (tasks != null && !tasks.isEmpty()) {
            tasks.sort(Comparator.comparing(Task::getCreatedAt));
            tasks.forEach(this::addToGraph);
            tasks.forEach(this::resolve);
        }
    }

    /**
     * re initialize task provider from task store
     */
    synchronized void reinit() {
        // clear in memory state
        clearGraph();
        // reinitialize in memory state from backing store
        init();
    }

    private void clearGraph() {
        logger.info("Clearing all tasks from task provider");
        final Set<Task> nodes = new HashSet<>(graph.nodes());
        nodes.forEach(graph::removeNode);
    }

    synchronized boolean add(Task task) {
        if (TaskStoreService.getService().load(task) == null) {
            addToGraph(task);
            TaskStoreService.getService().store(task);
            return true;
        } else {
            logger.warn("Task {} already exist with task provider, skip adding", task);
            return false;
        }
    }

    private void addToGraph(Task task) {
        graph.addNode(task);
    }

    /**
     * Check if all the dependant tasks for the given task are available and not in failed state.
     *
     * @param task
     * @return false when dependant tasks are in failed state or not found, true otherwise
     */
    synchronized boolean resolve(Task task) {
        final List<String> dependsOn = task.getDependsOn();
        if (dependsOn != null) {
            List<Task> dependentTasks = new ArrayList<>();
            for (String dependentTaskName : dependsOn) {
                Set<Task> tasks = getDependentTasks(dependentTaskName, task.getWorkflowId(), task.getNamespace());
                if (tasks.isEmpty()) {
                    logger.error("Missing tasks for dependency info {} for task {}", dependentTaskName, task);
                    return false;
                }
                // check if any of the dependee tasks is FAILED
                boolean isAnyDependeeFailed = tasks.stream().anyMatch(t -> t.getStatus().equals(FAILED));
                if (isAnyDependeeFailed) {
                    logger.error("Failed dependent tasks found for dependency info {} for task {}", dependentTaskName, task);
                    return false;
                }
                dependentTasks.addAll(tasks);
            }
            dependentTasks.forEach(dependentTask -> addDependency(dependentTask, task));
        }
        return true;
    }

    // used in junit test case
    Set<Task> getDependentTasks(String dependentTaskName, String workflowId, String namespace) {
        final TreeSet<Task> candidateDependentTasks = new TreeSet<>(Comparator.comparing(Task::getCreatedAt));

        // retrieve all dependent task in memory
        final List<Task> tasksInMemory = getTasks(dependentTaskName, workflowId, namespace);
        candidateDependentTasks.addAll(tasksInMemory);

        // retrieve all dependent task in store only if tasks in memory is empty
        if (candidateDependentTasks.isEmpty()) {
            final List<Task> tasksInStore = TaskStoreService.getService()
                    .loadByNameAndWorkflowId(dependentTaskName, workflowId, namespace);
            if (tasksInStore != null && !tasksInStore.isEmpty()) {
                candidateDependentTasks.addAll(tasksInStore);
            }
        }

        return candidateDependentTasks.isEmpty() ? Collections.emptySet() : candidateDependentTasks;
    }

    /**
     * For statement A depends on B, A is the depender and B is the dependee.
     *
     * @param dependerTask
     * @param dependeeTask
     */
    private void addDependency(Task dependerTask, Task dependeeTask) {
        graph.putEdge(dependerTask, dependeeTask);
    }

    synchronized Task getTask(TaskId taskId) {
        for (Task task : graph.nodes()) {
            if (task.getIdentity().equals(taskId)) {
                return task;
            }
        }
        Task task = TaskStoreService.getService().load(taskId);
        // update local cache if not null
        if (task != null) {
            addToGraph(task);
        }
        return task;
    }

    private List<Task> getTasks(String taskName, String workflowId, String namespace) {
        List<Task> tasks = new ArrayList<>();
        for (Task task : graph.nodes()) {
            if (task.getName().equals(taskName) && task.getWorkflowId().equals(workflowId)
                    && task.getNamespace().equals(namespace)) {
                tasks.add(task);
            }
        }
        return tasks;
    }

    synchronized List<Task> getReadyTasks() {
        final Predicate<Task> isReadyForExecution = this::isReadyForExecution;
        return getTasks(Collections.singletonList(WAITING), isReadyForExecution);
    }

    synchronized List<Task> getActiveTasks() {
        return getTasks(Arrays.asList(SCHEDULED, SUBMITTED, RUNNING));
    }

    synchronized List<Task> getDependentTasks(Task task) {
        return new ArrayList<>(graph.successors(task));
    }

    @SafeVarargs
    synchronized final List<Task> getTasks(List<Status> statuses, Predicate<Task>... predicates) {
        final Predicate<Task> statusPredicate = task -> statuses.contains(task.getStatus());
        if (predicates != null && predicates.length > 0) {
            final Predicate<Task> isReadyForExecutionPredicate = this::isReadyForExecution;
            return getTasks(statusPredicate, isReadyForExecutionPredicate);
        } else {
            return getTasks(statusPredicate);
        }
    }

    @SafeVarargs
    private final List<Task> getTasks(Predicate<Task>... predicates) {
        Stream<Task> stream = graph.nodes().stream();
        for (Predicate<Task> predicate : predicates) {
            stream = stream.filter(predicate);
        }
        return stream.collect(Collectors.toList());
    }

    private boolean isReadyForExecution(Task task) {
        return graph.predecessors(task)
                .stream()
                .allMatch(t -> t.getStatus().equals(SUCCESSFUL));
    }

    void update(Task task) {
        logger.debug("Received request to update task {}", task);
        TaskStoreService.getService().update(task);
    }

    /**
     * deletes all the old tasks from memory
     * task to delete is determined by {@link SchedulerConfig#taskPurgeInterval}
     * <p>
     * see: {@link SchedulerConfig#taskPurgeInterval} for more details and implication of taskPurgeInterval
     */
    synchronized void removeOldTasks(String taskPurgeInterval) {
        final Set<Task> tasksToDelete = new HashSet<>();

        Long cleanUpTimestamp = System.currentTimeMillis() - resolveDuration(taskPurgeInterval);
        for (Task task : graph.nodes()) {
            if (graph.inDegree(task) == 0) {
                tasksToDelete.addAll(getTasksToDelete(task, cleanUpTimestamp));
            }
        }

        logger.debug("Cleaning up tasks from memory {}", tasksToDelete);
        tasksToDelete.forEach(graph::removeNode);
    }

    /**
     * Returns Set of tasks for deletion if namespace of tasks (namespace is defined by connected depender or dependee task)
     * have been executed and trigger timestamp is older than cleanUpTimestamp.
     * <p>
     * Returns empty set if any of the tasks fails to match above mentioned criteria.
     */
    private Set<Task> getTasksToDelete(Task task, Long cleanUpTimestamp) {
        final Set<Task> tasksToDelete = new HashSet<>();
        final LinkedList<Task> tasksToValidate = new LinkedList<>();
        tasksToValidate.add(task);
        while (!tasksToValidate.isEmpty()) {
            Task taskToValidate = tasksToValidate.poll();
            if (taskToValidate != null && taskToValidate.getCreatedAt() < cleanUpTimestamp &&
                    (taskToValidate.getStatus() == SUCCESSFUL || taskToValidate.getStatus() == FAILED)) {
                Set<Task> predecessorTasks = graph.predecessors(taskToValidate);
                for (Task predecessorTask : predecessorTasks) {
                    if (!tasksToDelete.contains(predecessorTask)) {
                        tasksToValidate.add(predecessorTask);
                    }
                }
                Set<Task> successorTasks = graph.successors(taskToValidate);
                for (Task successorTask : successorTasks) {
                    if (!tasksToDelete.contains(successorTask)) {
                        tasksToValidate.add(successorTask);
                    }
                }
                tasksToDelete.add(taskToValidate);
            } else {
                return Collections.emptySet();
            }
        }
        return tasksToDelete;
    }

    // used in junit
    int size() {
        return graph.nodes().size();
    }

    public synchronized String toString() {
        StringBuilder graphOutputBuilder = new StringBuilder();
        graphOutputBuilder.append("Task Graph \n");
        for (Task task : graph.nodes()) {
            if (graph.inDegree(task) == 0) {
                prepareGraphOutput(task, 1, graphOutputBuilder);
            }
        }
        return graphOutputBuilder.toString();
    }

    private void prepareGraphOutput(Task task, int level, StringBuilder outputBuilder) {
        for (int i = 0; i < level; i++) {
            outputBuilder.append("  ");
        }
        outputBuilder.append("- ").append(task.getWorkflowId()).append(":").append(task.getName())
                .append(":").append(task.getId()).append("(").append(task.getStatus()).append(")\n");
        graph.successors(task).forEach(t -> prepareGraphOutput(t, level + 1, outputBuilder));
    }
}
