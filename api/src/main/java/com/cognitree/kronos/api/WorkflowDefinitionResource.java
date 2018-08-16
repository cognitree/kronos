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

import com.cognitree.kronos.model.definitions.WorkflowDefinition;
import com.cognitree.kronos.model.definitions.WorkflowDefinitionId;
import com.cognitree.kronos.scheduler.WorkflowSchedulerService;
import com.cognitree.kronos.scheduler.store.WorkflowDefinitionStoreService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

@Path("workflowdefs")
@Api(value = "workflow definition", description = "manage workflow definitions")
public class WorkflowDefinitionResource {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowDefinitionResource.class);

    private static final String DEFAULT_NAMESPACE = "default";

    @GET
    @ApiOperation(value = "Get all workflow definitions", response = WorkflowDefinition.class, responseContainer = "List")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllWorkflowDefinitions() {
        logger.info("Received request to get all workflow definitions");
        final List<WorkflowDefinition> workflowDefinitions = WorkflowDefinitionStoreService.getService().load();
        return Response.status(OK).entity(workflowDefinitions).build();
    }

    @GET
    @Path("{name}")
    @ApiOperation(value = "Get workflow definition with name", response = WorkflowDefinition.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Workflow definition not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflowDefinition(@ApiParam(value = "workflow definition name", required = true)
                                          @PathParam("name") String workflowDefName) {
        logger.info("Received request to get workflow definition with name {}", workflowDefName);
        WorkflowDefinitionId workflowDefinitionId = WorkflowDefinitionId.create(workflowDefName, DEFAULT_NAMESPACE);
        final WorkflowDefinition workflowDefinition =
                WorkflowDefinitionStoreService.getService().load(workflowDefinitionId);
        if (workflowDefinition == null) {
            logger.error("No workflow definition exists with name {}", workflowDefName);
            return Response.status(NOT_FOUND).build();
        }
        return Response.status(OK).entity(workflowDefinition).build();
    }

    @POST
    @ApiOperation(value = "Add new workflow definition", response = WorkflowDefinition.class)
    @ApiResponses(value = {
            @ApiResponse(code = 409, message = "Workflow definition already exists")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response addWorkflowDefinition(WorkflowDefinition workflowDefinition) {
        logger.info("Received request to add workflow definition {}", workflowDefinition);
        // override namespace of workflow
        workflowDefinition.setNamespace(DEFAULT_NAMESPACE);
        if (WorkflowDefinitionStoreService.getService().load(workflowDefinition) != null) {
            logger.error("Workflow definition already exists with name {}", workflowDefinition.getName());
            return Response.status(CONFLICT).build();
        }
        try {
            WorkflowSchedulerService.getService().add(workflowDefinition);
        } catch (Exception ex) {
            return Response.status(BAD_REQUEST).entity(ex.getMessage()).build();
        }
        WorkflowDefinitionStoreService.getService().store(workflowDefinition);
        return Response.status(OK).entity(workflowDefinition).build();
    }

    @PUT
    @Path("{name}")
    @ApiOperation(value = "Update workflow definition", response = WorkflowDefinition.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Workflow definition not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateWorkflowDefinition(@ApiParam(value = "workflow definition name", required = true)
                                             @PathParam("name") String workflowDefName,
                                             WorkflowDefinition workflowDefinition) {
        logger.info("Received request to update workflow definition with name {} to {}",
                workflowDefName, workflowDefinition);
        workflowDefinition.setName(workflowDefName);
        workflowDefinition.setNamespace(DEFAULT_NAMESPACE);
        if (WorkflowDefinitionStoreService.getService().load(workflowDefinition) == null) {
            logger.error("No workflow definition exists with name {}", workflowDefName);
            return Response.status(NOT_FOUND).build();
        }
        try {
            WorkflowSchedulerService.getService().update(workflowDefinition);
        } catch (Exception ex) {
            return Response.status(BAD_REQUEST).entity(ex.getMessage()).build();
        }
        WorkflowDefinitionStoreService.getService().update(workflowDefinition);
        return Response.status(OK).entity(workflowDefinition).build();
    }

    @DELETE
    @Path("{name}")
    @ApiOperation(value = "Delete workflow definition")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Workflow definition not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteWorkflowDefinition(@ApiParam(value = "workflow definition name", required = true)
                                             @PathParam("name") String workflowDefName) {
        logger.info("Received request to delete workflow definition with name {}", workflowDefName);
        WorkflowDefinitionId workflowDefinitionId = WorkflowDefinitionId.create(workflowDefName, DEFAULT_NAMESPACE);
        if (WorkflowDefinitionStoreService.getService().load(workflowDefinitionId) == null) {
            logger.error("No workflow definition exists with name {}", workflowDefName);
            return Response.status(NOT_FOUND).build();
        }
        WorkflowSchedulerService.getService().delete(workflowDefinitionId);
        WorkflowDefinitionStoreService.getService().delete(workflowDefinitionId);
        return Response.status(OK).build();
    }
}
