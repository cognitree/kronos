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
import com.cognitree.kronos.response.WorkflowResponse;
import com.cognitree.kronos.scheduler.NamespaceService;
import com.cognitree.kronos.scheduler.WorkflowService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

@Path("{res:workflows|w}")
@Api(value = "workflow", description = "manage workflows")
public class WorkflowResource {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowResource.class);
    private static final String DEFAULT_DAYS = "10";

    @GET
    @ApiOperation(value = "Get all running or executed workflows", response = Workflow.class, responseContainer = "List")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllWorkflows(@ApiParam(value = "workflow definition name")
                                    @QueryParam("name") String workflowName,
                                    @ApiParam(value = "workflow trigger name")
                                    @QueryParam("trigger") String triggerName,
                                    @ApiParam(value = "Number of days to fetch workflow from today", defaultValue = "10")
                                    @DefaultValue(DEFAULT_DAYS) @QueryParam("date_range") int numberOfDays,
                                    @HeaderParam("namespace") String namespace) {
        if (!validateNamespace(namespace)) {
            return Response.status(BAD_REQUEST).entity("no namespace exists with name " + namespace).build();
        }

        if (triggerName != null && workflowName == null) {
            return Response.status(BAD_REQUEST)
                    .entity("workflow definition name is mandatory if trigger name is specified").build();
        }

        final List<Workflow> workflows;
        if (triggerName != null) {
            logger.info("Received request to get all workflow for workflow definition {} submitted by workflow trigger {}" +
                            " in date range {} under namespace {}",
                    workflowName, triggerName, numberOfDays, namespace);
            workflows = WorkflowService.getService().get(workflowName, triggerName, namespace, numberOfDays);
        } else if (workflowName != null) {
            logger.info("Received request to get all workflow with name {}, date range {} under namespace {}",
                    workflowName, numberOfDays, namespace);
            workflows = WorkflowService.getService().get(workflowName, namespace, numberOfDays);
        } else {
            logger.info("Received request to get all workflow with date range {} under namespace {}",
                    numberOfDays, namespace);
            workflows = WorkflowService.getService().get(namespace, numberOfDays);
        }
        return Response.status(OK).entity(workflows.stream().sorted(comparing(Workflow::getCreatedAt).reversed())
                .collect(Collectors.toList())).build();
    }

    @GET
    @Path("{id}")
    @ApiOperation(value = "Get workflow with id", response = WorkflowResponse.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Workflow not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflow(@ApiParam(value = "workflow id", required = true)
                                @PathParam("id") String id,
                                @HeaderParam("namespace") String namespace) {
        logger.info("Received request to get workflow with id {} under namespace {}", id, namespace);
        if (!validateNamespace(namespace)) {
            return Response.status(BAD_REQUEST).entity("no namespace exists with name " + namespace).build();
        }
        WorkflowId workflowId = WorkflowId.build(id, namespace);
        final Workflow workflow = WorkflowService.getService().get(workflowId);
        if (workflow == null) {
            logger.error("No workflow exists with id {} under namespace {}", id, namespace);
            return Response.status(NOT_FOUND).build();
        }
        final List<Task> workflowTasks =
                WorkflowService.getService().getWorkflowTasks(workflow);

        return Response.status(OK).entity(WorkflowResponse.create(workflow, workflowTasks)).build();
    }

    private boolean validateNamespace(String name) {
        return name != null && NamespaceService.getService().get(name) != null;
    }
}
