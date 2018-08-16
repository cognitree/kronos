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

package com.cognitree.kronos.api;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.WorkflowId;
import com.cognitree.kronos.model.definitions.TaskDefinition;
import com.cognitree.kronos.model.definitions.WorkflowDefinition.WorkflowTask;
import com.cognitree.kronos.scheduler.store.WorkflowStoreService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

@Path("workflows")
@Api(value = "workflow", description = "manage workflow ")
public class WorkflowResource {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowResource.class);

    private static final String DEFAULT_NAMESPACE = "default";
    private static final String DEFAULT_DAYS = "10";

    @GET
    @ApiOperation(value = "Get all running or executed workflows", response = Workflow.class, responseContainer = "List")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllWorkflow(@ApiParam(value = "workflow name")
                                   @QueryParam("name") String workflowName,
                                   @ApiParam(value = "Number of days to fetch workflow from today", defaultValue = "10")
                                   @DefaultValue(DEFAULT_DAYS) @QueryParam("date_range") int numberOfDays) {
        final long currentTimeMillis = System.currentTimeMillis();
        long createdAfter = currentTimeMillis - (currentTimeMillis % TimeUnit.DAYS.toMillis(1))
                - TimeUnit.DAYS.toMillis(numberOfDays - 1);
        long createdBefore = createdAfter + TimeUnit.DAYS.toMillis(numberOfDays);
        final List<Workflow> workflows;
        if (workflowName == null) {
            logger.info("Received request to get all workflow with date range {}", numberOfDays);
            workflows = WorkflowStoreService.getService()
                    .load(DEFAULT_NAMESPACE, createdAfter, createdBefore);
        } else {
            logger.info("Received request to get all workflow with name {}, date range {}", workflowName, numberOfDays);
            workflows = WorkflowStoreService.getService()
                    .loadByName(workflowName, DEFAULT_NAMESPACE, createdAfter, createdBefore);
        }
        return Response.status(OK).entity(workflows.stream().sorted(comparing(Workflow::getCreatedAt).reversed())
                .collect(Collectors.toList())).build();
    }

    @GET
    @Path("{id}")
    @ApiOperation(value = "Get workflow with id", response = TaskDefinition.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Workflow not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflow(@ApiParam(value = "workflow id", required = true)
                                @PathParam("id") String id) {
        logger.info("Received request to get workflow with id {}", id);
        WorkflowId workflowId = WorkflowId.create(id, DEFAULT_NAMESPACE);
        final Workflow workflow = WorkflowStoreService.getService().load(workflowId);
        if (workflow == null) {
            logger.error("No workflow exists with id {}", id);
            return Response.status(NOT_FOUND).build();
        }
        return Response.status(OK).entity(workflow).build();
    }

    @GET
    @Path("{id}/tasks")
    @ApiOperation(value = "Get workflow tasks with id", response = Task.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Workflow not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflowTasks(@ApiParam(value = "workflow id", required = true)
                                     @PathParam("id") String id) {
        WorkflowId workflowId = WorkflowId.create(id, DEFAULT_NAMESPACE);
        final Workflow workflow = WorkflowStoreService.getService().load(workflowId);
        if (workflow == null) {
            logger.error("No workflow exists with id {}", workflowId);
            return Response.status(NOT_FOUND).build();
        }
        final List<Task> workflowTasks =
                WorkflowStoreService.getService().getWorkflowTasks(workflow);
        return Response.ok(workflowTasks).build();
    }

    @GET
    @Path("{id}/taskdefs")
    @ApiOperation(value = "Get workflow task for workflow", response = WorkflowTask.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Workflow not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflowTaskDefs(@ApiParam(value = "workflow id", required = true)
                                        @PathParam("id") String id) {
        WorkflowId workflowId = WorkflowId.create(id, DEFAULT_NAMESPACE);
        final Workflow workflow = WorkflowStoreService.getService().load(workflowId);
        if (workflow == null) {
            logger.error("No workflow exists with id {}", workflowId);
            return Response.status(NOT_FOUND).build();
        }
        final List<WorkflowTask> workflowTaskDefs =
                WorkflowStoreService.getService().getWorkflowTaskDefs(workflow);
        return Response.ok(workflowTaskDefs).build();
    }
}
