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

import com.cognitree.kronos.model.MutableTaskId;
import com.cognitree.kronos.model.Namespace;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskId;
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

    void init() throws ServiceException {
        logger.info("Initializing task provider from task store");
        final List<Namespace> namespaces = NamespaceService.getService().get();
        final List<Task> tasks = new ArrayList<>();
        for (Namespace namespace : namespaces) {
            TaskService.getService()
                    .get(Arrays.asList(CREATED, WAITING, SCHEDULED, SUBMITTED, RUNNING), namespace.getName());
        }
        if (!tasks.isEmpty()) {
            tasks.sort(Comparator.comparing(Task::getCreatedAt));
            tasks.forEach(this::addToGraph);
            for (Task task : tasks) {
                resolve(task);
            }
        }
    }

    /**
     * re initialize task provider from task store
     */
    synchronized void reinit() throws ServiceException {
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

    synchronized boolean add(Task task) throws ServiceException {
        if (TaskService.getService().get(task) == null) {
            addToGraph(task);
            TaskService.getService().add(task);
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
    synchronized boolean resolve(Task task) throws ServiceException {
        final List<String> dependsOn = task.getDependsOn();
        if (dependsOn != null) {
            List<Task> dependentTasks = new ArrayList<>();
            for (String dependentTaskName : dependsOn) {
                TaskId dependentTaskId = MutableTaskId.build(dependentTaskName, task.getJob(), task.getNamespace());
                Task dependentTask = getTask(dependentTaskId);
                if (dependentTask == null) {
                    logger.error("No dependent task with id {} not found", dependentTaskId);
                    return false;
                }

                if (dependentTask.getStatus() == FAILED) {
                    logger.error("Dependent task with id {} is in failed state", dependentTaskId);
                    return false;
                }
                dependentTasks.add(dependentTask);
            }
            dependentTasks.forEach(dependentTask -> addDependency(dependentTask, task));
        }
        return true;
    }

    synchronized Task getTask(TaskId taskId) throws ServiceException {
        for (Task task : graph.nodes()) {
            if (task.getIdentity().equals(taskId)) {
                return task;
            }
        }
        Task task = TaskService.getService().get(taskId);
        // update local cache if not null
        if (task != null) {
            addToGraph(task);
        }
        return task;
    }

    /**
     * For statement A depends on B, A is the dependent and B is the dependee.
     *
     * @param dependentTask
     * @param dependeeTask
     */
    private void addDependency(Task dependentTask, Task dependeeTask) {
        graph.putEdge(dependentTask, dependeeTask);
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

    void update(Task task) throws ServiceException {
        logger.debug("Received request to update task {}", task);
        TaskService.getService().update(task);
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
     * Returns Set of tasks for deletion if namespace of tasks (namespace is defined by connected dependent or dependee task)
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
                    (taskToValidate.getStatus().isFinal())) {
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
        outputBuilder.append("- ").append(task.getJob()).append(":").append(task.getName())
                .append(":").append(task.getName()).append("(").append(task.getStatus()).append(")\n");
        graph.successors(task).forEach(t -> prepareGraphOutput(t, level + 1, outputBuilder));
    }
}
