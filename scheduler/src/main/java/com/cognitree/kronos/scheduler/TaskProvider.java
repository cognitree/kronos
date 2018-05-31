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

import com.cognitree.kronos.ApplicationConfig;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Task.Status;
import com.cognitree.kronos.model.TaskDependencyInfo;
import com.cognitree.kronos.store.TaskStore;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.cognitree.kronos.model.Task.Status.*;
import static com.cognitree.kronos.model.TaskDependencyInfo.Mode.first;
import static com.cognitree.kronos.util.DateTimeUtil.resolveDuration;

/**
 * Task provider manages/ resolves task dependencies and exposes APIs to add, remove, retrieve tasks in active and
 * ready-to-execute state
 * listen for task status updates and calls the registered {@link TaskStatusChangeListener} on status change.
 * <p>
 * Internally, task provider is backed by a directed acyclic graph that manages dependencies across these tasks.
 * The task state is stored in a persistent store if provided.
 */
class TaskProvider {
    private static final Logger logger = LoggerFactory.getLogger(TaskProvider.class);

    private final MutableGraph<Task> graph = GraphBuilder.directed().build();
    private final TaskStore taskStore;

    TaskProvider(TaskStore store) {
        this.taskStore = store;
        init();
    }

    private void init() {
        logger.info("Initializing task provider from task store");
        final List<Task> tasks = taskStore.load(Arrays.asList(CREATED, WAITING, SUBMITTED, RUNNING));
        if (tasks != null && !tasks.isEmpty()) {
            tasks.sort(Comparator.comparing(Task::getCreatedAt));
            tasks.forEach(this::addToGraph);
            tasks.forEach(this::resolveAndUpdateDependency);
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

    synchronized void add(Task task) {
        if (taskStore.load(task.getId(), task.getGroup()) == null) {
            addToGraph(task);
            taskStore.store(task);
        } else {
            logger.warn("Task {} already exist with task provider, skip adding", task);
        }
    }

    private void addToGraph(Task task) {
        graph.addNode(task);
    }

    /**
     * Check if all the dependant tasks of the given task are available and not in failed state.
     *
     * @param task
     * @return false when dependant tasks are in failed state or not found, true otherwise
     */
    boolean resolveAndUpdateDependency(Task task) {
        final List<TaskDependencyInfo> dependencyInfoList = task.getDependsOn();
        if (dependencyInfoList != null) {
            List<Task> dependentTasks = new ArrayList<>();
            for (TaskDependencyInfo dependencyInfo : dependencyInfoList) {
                Set<Task> tasks = getDependentTasks(task, dependencyInfo);
                if (tasks.isEmpty()) {
                    logger.error("Missing tasks for dependency info {} for task {}", dependencyInfo, task);
                    return false;
                }
                // check if any of the dependee tasks is FAILED
                boolean isAnyDependeeFailed = tasks.stream().anyMatch(t -> t.getStatus().equals(FAILED));
                if (isAnyDependeeFailed) {
                    logger.error("Failed dependent tasks found for dependency info {} for task {}", dependencyInfo, task);
                    return false;
                }
                dependentTasks.addAll(tasks);
            }
            dependentTasks.forEach(dependentTask -> addDependency(dependentTask, task));
        }
        return true;
    }

    // used in junit test case
    Set<Task> getDependentTasks(Task task, TaskDependencyInfo dependencyInfo) {
        final TreeSet<Task> candidateDependentTasks = new TreeSet<>(Comparator.comparing(Task::getCreatedAt));
        final long createdAt = task.getCreatedAt();
        final long sentinelTimeStamp = createdAt - resolveDuration(dependencyInfo.getDuration());
        final String taskGroup = task.getGroup();
        final String dependentTaskName = dependencyInfo.getName();

        // retrieve all dependent task in memory
        final List<Task> tasksInMemory = getTasks(dependentTaskName, taskGroup, createdAt, sentinelTimeStamp);
        candidateDependentTasks.addAll(tasksInMemory);

        // retrieve all dependent task in store only if dependency mode is not first and tasks in memory is empty
        if (candidateDependentTasks.isEmpty() || dependencyInfo.getMode() != first) {
            final List<Task> tasksInStore = taskStore.load(dependentTaskName, taskGroup, createdAt, sentinelTimeStamp);
            if (tasksInStore != null && !tasksInStore.isEmpty()) {
                candidateDependentTasks.addAll(tasksInStore);
            }
        }

        if (candidateDependentTasks.isEmpty()) return Collections.emptySet();

        Set<Task> dependentTasks;
        switch (dependencyInfo.getMode()) {
            case first:
                dependentTasks = Collections.singleton(candidateDependentTasks.first());
                break;
            case last:
                dependentTasks = Collections.singleton(candidateDependentTasks.last());
                break;
            case all:
                dependentTasks = candidateDependentTasks;
                break;
            default:
                dependentTasks = Collections.emptySet();
        }
        return dependentTasks;
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

    Task getTask(String taskId, String taskGroup) {
        for (Task task : graph.nodes()) {
            if (task.getId().equals(taskId) && task.getGroup().equals(taskGroup)) {
                return task;
            }
        }
        Task task = taskStore.load(taskId, taskGroup);
        // update local cache if not null
        if (task != null) {
            addToGraph(task);
        }
        return task;
    }

    private List<Task> getTasks(String taskName, String taskGroup, long createdBefore, long createdAfter) {
        List<Task> tasks = new ArrayList<>();
        for (Task task : graph.nodes()) {
            if (task.getName().equals(taskName) && task.getGroup().equals(taskGroup)) {
                if (task.getCreatedAt() < createdBefore && task.getCreatedAt() > createdAfter) {
                    tasks.add(task);
                }
            }
        }
        return tasks;
    }

    synchronized List<Task> getReadyTasks() {
        final Predicate<Task> isReadyForExecution = this::isReadyForExecution;
        return getTasks(Collections.singletonList(WAITING), isReadyForExecution);
    }

    synchronized List<Task> getActiveTasks() {
        return getTasks(Arrays.asList(SUBMITTED, RUNNING));
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

    boolean isReadyForExecution(Task task) {
        return graph.predecessors(task)
                .stream()
                .allMatch(t -> t.getStatus().equals(SUCCESSFUL));
    }

    /**
     * deletes all the old tasks from memory
     * task to delete is determined by {@link ApplicationConfig#taskPurgeInterval}
     * <p>
     * see: {@link ApplicationConfig#taskPurgeInterval} for more details and implication of taskPurgeInterval
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
     * Returns Set of tasks for deletion if group of tasks (group is defined by connected depender or dependee task)
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
        outputBuilder.append("- ").append(task.getGroup()).append(":").append(task.getName())
                .append(":").append(task.getId()).append("(").append(task.getStatus()).append(")\n");
        graph.successors(task).forEach(t -> prepareGraphOutput(t, level + 1, outputBuilder));
    }
}
