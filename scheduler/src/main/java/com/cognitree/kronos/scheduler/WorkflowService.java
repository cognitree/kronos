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

import com.cognitree.kronos.Service;
import com.cognitree.kronos.ServiceException;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.model.Policy;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.scheduler.graph.TopologicalSort;
import com.cognitree.kronos.scheduler.model.ExecutionCounters;
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.NamespaceId;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowId;
import com.cognitree.kronos.scheduler.model.WorkflowStatistics;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.store.StoreException;
import com.cognitree.kronos.scheduler.store.StoreService;
import com.cognitree.kronos.scheduler.store.WorkflowStore;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.cognitree.kronos.scheduler.ValidationError.CYCLIC_DEPENDENCY_IN_WORKFLOW;
import static com.cognitree.kronos.scheduler.ValidationError.DUPLICATE_POLICY_OF_SAME_TYPE;
import static com.cognitree.kronos.scheduler.ValidationError.MISSING_PARAM_IN_WORKFLOW;
import static com.cognitree.kronos.scheduler.ValidationError.MISSING_TASK_IN_WORKFLOW;
import static com.cognitree.kronos.scheduler.ValidationError.NAMESPACE_NOT_FOUND;
import static com.cognitree.kronos.scheduler.ValidationError.WORKFLOW_ALREADY_EXISTS;
import static com.cognitree.kronos.scheduler.ValidationError.WORKFLOW_NOT_FOUND;
import static com.cognitree.kronos.scheduler.model.Constants.DYNAMIC_VAR_PREFIX;
import static com.cognitree.kronos.scheduler.model.Constants.DYNAMIC_VAR_SUFFFIX;
import static com.cognitree.kronos.scheduler.model.Constants.WORKFLOW_NAMESPACE_PREFIX;

public class WorkflowService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowService.class);

    private static final List<Job.Status> ACTIVE_JOB_STATUS = new ArrayList<>();
    private static final List<Task.Status> ACTIVE_TASK_STATUS = new ArrayList<>();

    static {
        for (Job.Status status : Job.Status.values()) {
            if (!status.isFinal()) {
                ACTIVE_JOB_STATUS.add(status);
            }
        }
        for (Task.Status status : Task.Status.values()) {
            if (!status.isFinal()) {
                ACTIVE_TASK_STATUS.add(status);
            }
        }
    }

    private WorkflowStore workflowStore;

    public static WorkflowService getService() {
        return (WorkflowService) ServiceProvider.getService(WorkflowService.class.getSimpleName());
    }

    @Override
    public void init() {
        logger.info("Initializing workflow service");
    }

    @Override
    public void start() {
        logger.info("Starting workflow service");
        StoreService storeService = (StoreService) ServiceProvider.getService(StoreService.class.getSimpleName());
        workflowStore = storeService.getWorkflowStore();
        ServiceProvider.registerService(this);
    }

    public void add(Workflow workflow) throws ValidationException, ServiceException {
        logger.info("Received request to add workflow {}", workflow);
        validateNamespace(workflow.getNamespace());
        validate(workflow);
        try {
            if (workflowStore.load(workflow) != null) {
                throw WORKFLOW_ALREADY_EXISTS.createException(workflow.getName(), workflow.getNamespace());
            }
            WorkflowSchedulerService.getService().add(workflow);
            workflowStore.store(workflow);
        } catch (StoreException | SchedulerException e) {
            logger.error("unable to add workflow {}", workflow, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public List<Workflow> get(String namespace) throws ServiceException, ValidationException {
        logger.debug("Received request to get all workflow under namespace {}", namespace);
        validateNamespace(namespace);
        try {
            final List<Workflow> workflows = workflowStore.load(namespace);
            return workflows == null ? Collections.emptyList() : workflows;
        } catch (StoreException e) {
            logger.error("unable to get all workflow under namespace {}", namespace, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public Workflow get(WorkflowId workflowId) throws ServiceException, ValidationException {
        logger.debug("Received request to get workflow {}", workflowId);
        validateNamespace(workflowId.getNamespace());
        try {
            return workflowStore.load(workflowId);
        } catch (StoreException e) {
            logger.error("unable to get workflow {}", workflowId, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public WorkflowStatistics getStatistics(String namespace, long createdAfter, long createdBefore)
            throws ValidationException, ServiceException {
        logger.debug("Received request to get all jobs statistics under namespace {} created " +
                "between {} to {}", namespace, createdAfter, createdBefore);
        validateNamespace(namespace);
        final Map<Job.Status, Integer> jobStatusMap =
                JobService.getService().countByStatus(namespace, createdAfter, createdBefore);
        final Map<Task.Status, Integer> taskStatusMap = TaskService.getService().countByStatus(namespace, createdAfter, createdBefore);
        return getWorkflowStatistics(jobStatusMap, taskStatusMap, createdAfter, createdBefore);
    }

    public WorkflowStatistics getStatistics(String namespace, String workflowName, long createdAfter, long createdBefore)
            throws ValidationException, ServiceException {
        logger.debug("Received request to get statistics for jobs with name {} under namespace {} created " +
                "between {} to {}", workflowName, namespace, createdAfter, createdBefore);
        validateNamespace(namespace);
        final Map<Job.Status, Integer> jobStatusMap =
                JobService.getService().countByStatus(namespace, workflowName, createdAfter, createdBefore);
        final Map<Task.Status, Integer> taskStatusMap =
                TaskService.getService().countByStatus(namespace, workflowName, createdAfter, createdBefore);
        return getWorkflowStatistics(jobStatusMap, taskStatusMap, createdAfter, createdBefore);
    }

    private WorkflowStatistics getWorkflowStatistics(Map<Job.Status, Integer> jobStatusMap, Map<Task.Status, Integer> taskStatusMap,
                                                     long createdAfter, long createdBefore) {
        WorkflowStatistics workflowStatistics = new WorkflowStatistics();
        ExecutionCounters jobExecutionCounters = new ExecutionCounters();
        jobExecutionCounters.setTotal(jobStatusMap.values().stream().mapToInt(Integer::intValue).sum());
        int activeJobs = 0;
        for (Job.Status status : ACTIVE_JOB_STATUS) {
            if (jobStatusMap.containsKey(status)) {
                activeJobs += jobStatusMap.get(status);
            }
        }
        jobExecutionCounters.setActive(activeJobs);
        jobExecutionCounters.setSuccessful(jobStatusMap.getOrDefault(Job.Status.SUCCESSFUL, 0));
        jobExecutionCounters.setFailed(jobStatusMap.getOrDefault(Job.Status.FAILED, 0));
        workflowStatistics.setJobs(jobExecutionCounters);

        ExecutionCounters taskExecutionCounters = new ExecutionCounters();
        taskExecutionCounters.setTotal(taskStatusMap.values().stream().mapToInt(Integer::intValue).sum());
        int activeTasks = 0;
        for (Task.Status status : ACTIVE_TASK_STATUS) {
            if (taskStatusMap.containsKey(status)) {
                activeTasks += taskStatusMap.get(status);
            }
        }
        taskExecutionCounters.setActive(activeTasks);
        taskExecutionCounters.setSuccessful(taskStatusMap.getOrDefault(Task.Status.SUCCESSFUL, 0));
        taskExecutionCounters.setFailed(taskStatusMap.getOrDefault(Task.Status.FAILED, 0));
        taskExecutionCounters.setSkipped(taskStatusMap.getOrDefault(Task.Status.SKIPPED, 0));
        taskExecutionCounters.setStopped(taskStatusMap.getOrDefault(Task.Status.STOPPED, 0));
        workflowStatistics.setTasks(taskExecutionCounters);

        workflowStatistics.setFrom(createdAfter);
        workflowStatistics.setTo(createdBefore);
        return workflowStatistics;
    }


    public void update(Workflow workflow) throws ValidationException, ServiceException {
        logger.info("Received request to update workflow {}", workflow);
        validateNamespace(workflow.getNamespace());
        validate(workflow);
        try {
            if (workflowStore.load(workflow) == null) {
                throw WORKFLOW_NOT_FOUND.createException(workflow.getName(), workflow.getNamespace());
            }
            WorkflowSchedulerService.getService().update(workflow);
            workflowStore.update(workflow);
        } catch (StoreException | SchedulerException e) {
            logger.error("unable to update workflow {}", workflow, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }
    }

    public void delete(WorkflowId workflowId) throws SchedulerException, ServiceException, ValidationException {
        logger.info("Received request to delete workflow {}", workflowId);
        validateNamespace(workflowId.getNamespace());
        try {
            if (workflowStore.load(workflowId) == null) {
                throw WORKFLOW_NOT_FOUND.createException(workflowId.getName(), workflowId.getNamespace());
            }
            // delete all triggers before deleting workflow
            final List<WorkflowTrigger> workflowTriggers =
                    WorkflowTriggerService.getService().get(workflowId.getNamespace(), workflowId.getName());
            for (WorkflowTrigger workflowTrigger : workflowTriggers) {
                WorkflowTriggerService.getService().delete(workflowTrigger);
            }
            // delete all workflow jobs before deleting workflow
            JobService.getService().delete(workflowId.getNamespace(), workflowId.getName());
            WorkflowSchedulerService.getService().delete(workflowId);
            workflowStore.delete(workflowId);
        } catch (StoreException e) {
            logger.error("unable to delete workflow {}", workflowId, e);
            throw new ServiceException(e.getMessage(), e.getCause());
        }

    }

    private void validateNamespace(String name) throws ValidationException, ServiceException {
        final Namespace namespace = NamespaceService.getService().get(NamespaceId.build(name));
        if (namespace == null) {
            throw NAMESPACE_NOT_FOUND.createException(name);
        }
    }

    /**
     * validate workflow
     *
     * @param workflow
     * @return
     */
    private void validate(Workflow workflow) throws ValidationException {
        final HashMap<String, Workflow.WorkflowTask> workflowTaskMap = new HashMap<>();
        final TopologicalSort<Workflow.WorkflowTask> topologicalSort = new TopologicalSort<>();
        final List<Workflow.WorkflowTask> workflowTasks = workflow.getTasks();
        for (Workflow.WorkflowTask task : workflowTasks) {
            if (task.isEnabled()) {
                workflowTaskMap.put(task.getName(), task);
                topologicalSort.add(task);
            }
        }

        for (Workflow.WorkflowTask workflowTask : workflowTasks) {
            final List<String> dependsOn = workflowTask.getDependsOn();
            if (dependsOn != null && !dependsOn.isEmpty()) {
                for (String dependentTask : dependsOn) {
                    final Workflow.WorkflowTask dependeeTask = workflowTaskMap.get(dependentTask);
                    if (dependeeTask == null) {
                        throw MISSING_TASK_IN_WORKFLOW.createException(dependentTask);
                    }
                    topologicalSort.add(dependeeTask, workflowTask);
                }
            }
        }
        if (!topologicalSort.isDag()) {
            throw CYCLIC_DEPENDENCY_IN_WORKFLOW.createException();
        }

        final Map<String, Object> workflowProperties = workflow.getProperties();
        for (Workflow.WorkflowTask workflowTask : workflowTasks) {
            validateWorkflowProperties(workflowTask.getName(), workflowTask.getProperties(), workflowProperties);
            validateWorkflowTaskPolicies(workflowTask.getName(), workflowTask.getPolicies());
        }
    }

    private void validateWorkflowProperties(String workflowTask, Map<String, Object> taskProperties,
                                            Map<String, Object> workflowProperties) throws ValidationException {
        for (Map.Entry<String, Object> entry : taskProperties.entrySet()) {
            final Object value = entry.getValue();
            if (value instanceof String &&
                    ((String) value).startsWith(DYNAMIC_VAR_PREFIX) &&
                    ((String) value).endsWith(DYNAMIC_VAR_SUFFFIX) &&
                    ((String) value).contains(WORKFLOW_NAMESPACE_PREFIX)) {
                String valueToReplace = ((String) value).substring(DYNAMIC_VAR_PREFIX.length(),
                        ((String) value).length() - DYNAMIC_VAR_SUFFFIX.length()).trim();
                valueToReplace = valueToReplace.substring(WORKFLOW_NAMESPACE_PREFIX.length());
                if (!workflowProperties.containsKey(valueToReplace)) {
                    throw MISSING_PARAM_IN_WORKFLOW.createException(valueToReplace, workflowTask);
                }
            } else if (value instanceof Map) {
                validateWorkflowProperties(workflowTask, (Map<String, Object>) value, workflowProperties);
            }
        }
    }

    private void validateWorkflowTaskPolicies(String workflowTask, List<Policy> policies) throws ValidationException {
        Map<Policy.Type, List<Policy>> policyTypeMap = new HashMap<>();
        for (Policy policy : policies) {
            List<Policy> policyTypeList =
                    policyTypeMap.computeIfAbsent(policy.getType(), k -> new ArrayList<>());
            policyTypeList.add(policy);
        }
        for (Map.Entry<Policy.Type, List<Policy>> entry : policyTypeMap.entrySet()) {
            Policy.Type type = entry.getKey();
            List<Policy> policyTypeList = entry.getValue();
            if (policyTypeList.size() > 1) {
                throw DUPLICATE_POLICY_OF_SAME_TYPE.createException(workflowTask, type);
            }
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping workflow service");
    }
}