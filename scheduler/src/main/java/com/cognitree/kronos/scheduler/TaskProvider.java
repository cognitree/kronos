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
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.cognitree.kronos.model.Task.Status.FAILED;
import static com.cognitree.kronos.model.Task.Status.RUNNING;
import static com.cognitree.kronos.model.Task.Status.SCHEDULED;
import static com.cognitree.kronos.model.Task.Status.SUBMITTED;
import static com.cognitree.kronos.model.Task.Status.SUCCESSFUL;
import static com.cognitree.kronos.model.Task.Status.UP_FOR_RETRY;
import static com.cognitree.kronos.model.Task.Status.WAITING;

/**
 * Task provider manages/ resolves task dependencies and exposes APIs to add, remove, retrieve tasks in active and
 * ready-to-execute state.
 * <p>
 * Internally, task provider is backed by a directed acyclic graph to manage dependencies across these tasks.
 */
final class TaskProvider {
    private static final Logger logger = LoggerFactory.getLogger(TaskProvider.class);

    private final MutableGraph<Task> graph = GraphBuilder.directed().build();

    synchronized boolean add(Task task) {
        return graph.addNode(task);
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
                TaskId dependentTaskId = TaskId.build(task.getNamespace(), dependentTaskName, task.getJob(),
                        task.getWorkflow());
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

    synchronized Task getTask(TaskId taskId) {
        for (Task task : graph.nodes()) {
            if (task.getIdentity().equals(taskId)) {
                return task;
            }
        }
        return null;
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
        return getTasks(Arrays.asList(UP_FOR_RETRY, WAITING), isReadyForExecution);
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

    /**
     * deletes all the stale tasks from graph having `createdAt` older than `durationInMillis`
     * </p>
     * before deleting a task we assure that the task is no longer required in future
     */
    synchronized void removeStaleTasks(long durationInMillis) {
        final Set<Task> tasksToDelete = new HashSet<>();

        Long cleanUpTimestamp = System.currentTimeMillis() - durationInMillis;
        for (Task task : graph.nodes()) {
            if (graph.inDegree(task) == 0) {
                tasksToDelete.addAll(getTasksToDelete(task, cleanUpTimestamp));
            }
        }

        logger.debug("Cleaning up tasks from memory {}", tasksToDelete);
        tasksToDelete.forEach(graph::removeNode);
    }

    /**
     * At any point in time a graph should contain all the tasks in a workflow.
     * Tasks are returned only if the entire workflow(job) it belongs to is complete
     * and is having `createdAt` older than the `cleanupTimestamp`
     */
    private Set<Task> getTasksToDelete(Task task, Long cleanupTimestamp) {
        final Set<Task> tasksToDelete = new HashSet<>();
        final LinkedList<Task> tasksToValidate = new LinkedList<>();
        tasksToValidate.add(task);
        while (!tasksToValidate.isEmpty()) {
            Task taskToValidate = tasksToValidate.poll();
            if (taskToValidate != null && taskToValidate.getCreatedAt() < cleanupTimestamp &&
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
}
