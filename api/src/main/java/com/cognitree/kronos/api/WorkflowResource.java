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

import com.cognitree.kronos.scheduler.ServiceException;
import com.cognitree.kronos.scheduler.ValidationException;
import com.cognitree.kronos.scheduler.WorkflowService;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowId;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

@Path("workflows")
@Api(value = "workflows", description = "manage workflows")
public class WorkflowResource {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowResource.class);

    @GET
    @ApiOperation(value = "Get all workflows", response = Workflow.class, responseContainer = "List")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllWorkflows(@HeaderParam("namespace") String namespace) throws ServiceException, ValidationException {
        logger.info("Received request to get all workflows under namespace {}", namespace);
        if (namespace == null || namespace.isEmpty()) {
            return Response.status(BAD_REQUEST).entity("missing namespace header").build();
        }
        final List<Workflow> workflows = WorkflowService.getService().get(namespace);
        return Response.status(OK).entity(workflows).build();
    }

    @GET
    @Path("{name}")
    @ApiOperation(value = "Get workflow by name", response = Workflow.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Workflow not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflow(@ApiParam(value = "workflow name", required = true)
                                @PathParam("name") String name,
                                @HeaderParam("namespace") String namespace) throws ServiceException, ValidationException {
        logger.info("Received request to get workflow with name {} under namespace {}", name, namespace);
        if (namespace == null || namespace.isEmpty()) {
            return Response.status(BAD_REQUEST).entity("missing namespace header").build();
        }
        WorkflowId workflowId = WorkflowId.build(namespace, name);
        final Workflow workflow = WorkflowService.getService().get(workflowId);
        if (workflow == null) {
            logger.error("No workflow exists with name {} under namespace {}", name, namespace);
            return Response.status(NOT_FOUND).build();
        }
        return Response.status(OK).entity(workflow).build();
    }

    @POST
    @ApiOperation(value = "Add new workflow", response = Workflow.class)
    @ApiResponses(value = {
            @ApiResponse(code = 409, message = "Workflow already exists")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response addWorkflow(@HeaderParam("namespace") String namespace, Workflow workflow) throws ServiceException, ValidationException {
        workflow.setNamespace(namespace);
        logger.info("Received request to add workflow {} under namespace {}", workflow, namespace);
        if (namespace == null || namespace.isEmpty()) {
            return Response.status(BAD_REQUEST).entity("missing namespace header").build();
        }
        WorkflowService.getService().add(workflow);
        return Response.status(CREATED).entity(workflow).build();
    }

    @PUT
    @Path("{name}")
    @ApiOperation(value = "Update workflow by name", response = Workflow.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Workflow not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateWorkflow(@ApiParam(value = "workflow name", required = true)
                                   @PathParam("name") String name,
                                   @HeaderParam("namespace") String namespace,
                                   Workflow workflow) throws ServiceException, ValidationException {
        workflow.setName(name);
        workflow.setNamespace(namespace);
        logger.info("Received request to update workflow with name {} under namespace {} to {}",
                name, namespace, workflow);
        if (namespace == null || namespace.isEmpty()) {
            return Response.status(BAD_REQUEST).entity("missing namespace header").build();
        }
        WorkflowService.getService().update(workflow);
        return Response.status(OK).entity(workflow).build();
    }

    @DELETE
    @Path("{name}")
    @ApiOperation(value = "Delete workflow by name")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Workflow not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteWorkflow(@ApiParam(value = "workflow name", required = true)
                                   @PathParam("name") String name,
                                   @HeaderParam("namespace") String namespace) throws ServiceException, SchedulerException, ValidationException {
        logger.info("Received request to delete workflow with name {} under namespace {}", name, namespace);
        if (namespace == null || namespace.isEmpty()) {
            return Response.status(BAD_REQUEST).entity("missing namespace header").build();
        }
        WorkflowId workflowId = WorkflowId.build(namespace, name);
        WorkflowService.getService().delete(workflowId);
        return Response.status(OK).build();
    }
}
