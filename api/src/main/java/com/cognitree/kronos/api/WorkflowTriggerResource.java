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

import com.cognitree.kronos.ServiceException;
import com.cognitree.kronos.scheduler.ValidationException;
import com.cognitree.kronos.scheduler.WorkflowTriggerService;
import com.cognitree.kronos.scheduler.model.WorkflowId;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.model.WorkflowTriggerId;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

@Path("workflows/{workflow}/triggers")
@Api(value = "workflow triggers", description = "manage workflow triggers")
public class WorkflowTriggerResource {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowTriggerResource.class);

    @GET
    @ApiOperation(value = "Get all workflow triggers", response = WorkflowTrigger.class, responseContainer = "List")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllWorkflowTriggers(@PathParam("workflow") String workflowName,
                                           @QueryParam("enable") Boolean enable,
                                           @HeaderParam("namespace") String namespace)
            throws ServiceException, ValidationException {
        logger.info("Received request to get all workflow triggers for workflow {} with query param enable {} under namespace {}",
                workflowName, enable, namespace);
        if (namespace == null || namespace.isEmpty()) {
            return Response.status(BAD_REQUEST).entity("missing namespace header").build();
        }
        final List<WorkflowTrigger> triggers;
        if (enable == null) {
            triggers = WorkflowTriggerService.getService().get(namespace, workflowName);
        } else {
            triggers = WorkflowTriggerService.getService().get(namespace, workflowName, enable);
        }
        return Response.status(OK).entity(triggers).build();
    }

    @GET
    @Path("/{name}")
    @ApiOperation(value = "Get workflow trigger by name", response = WorkflowTrigger.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Workflow trigger not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflowTrigger(@ApiParam(value = "workflow name", required = true)
                                       @PathParam("workflow") String workflowName,
                                       @ApiParam(value = "workflow trigger name", required = true)
                                       @PathParam("name") String triggerName,
                                       @HeaderParam("namespace") String namespace) throws ServiceException, ValidationException {
        logger.info("Received request to get workflow trigger with name {} for workflow {} under namespace {}",
                triggerName, workflowName, namespace);
        if (namespace == null || namespace.isEmpty()) {
            return Response.status(BAD_REQUEST).entity("missing namespace header").build();
        }
        WorkflowTriggerId triggerId = WorkflowTriggerId.build(namespace, triggerName, workflowName);
        final WorkflowTrigger workflowTrigger = WorkflowTriggerService.getService().get(triggerId);
        if (workflowTrigger == null) {
            logger.error("No workflow trigger exists with name {} under namespace {}", triggerName, namespace);
            return Response.status(NOT_FOUND).build();
        }
        return Response.status(OK).entity(workflowTrigger).build();
    }

    @POST
    @ApiOperation(value = "Create workflow trigger", response = WorkflowTrigger.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Workflow trigger not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response createWorkflowTrigger(@ApiParam(value = "workflow name", required = true)
                                          @PathParam("workflow") String workflowName,
                                          @HeaderParam("namespace") String namespace,
                                          WorkflowTrigger workflowTrigger) throws ServiceException, ValidationException, SchedulerException {
        // override workflow name and namespace and enabled flag
        workflowTrigger.setWorkflow(workflowName);
        workflowTrigger.setNamespace(namespace);
        workflowTrigger.setEnabled(true);
        logger.info("Received request to create workflow trigger {} for workflow {} under namespace {}",
                workflowTrigger, workflowName, namespace);
        if (namespace == null || namespace.isEmpty()) {
            return Response.status(BAD_REQUEST).entity("missing namespace header").build();
        }
        WorkflowTriggerService.getService().add(workflowTrigger);
        return Response.status(CREATED).entity(workflowTrigger).build();
    }

    @PUT
    @ApiOperation(value = "Enable/ Disable all triggers for workflow by name", response = WorkflowTrigger.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "list of affected triggers"),
            @ApiResponse(code = 404, message = "Workflow not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateWorkflow(@ApiParam(value = "workflow name", required = true)
                                   @PathParam("workflow") String workflowName,
                                   @ApiParam(value = "enable/ disable all workflow triggers", required = true)
                                   @DefaultValue("true") @QueryParam("enable") boolean enable,
                                   @HeaderParam("namespace") String namespace)
            throws ServiceException, ValidationException, SchedulerException {
        logger.info("Received request to update all triggers for workflow with name {} under namespace {} set enable to {}",
                workflowName, namespace, enable);
        if (namespace == null || namespace.isEmpty()) {
            return Response.status(BAD_REQUEST).entity("missing namespace header").build();
        }
        final List<WorkflowTrigger> workflowTriggers;
        if (enable) {
            workflowTriggers = WorkflowTriggerService.getService().resume(WorkflowId.build(namespace, workflowName));
        } else {
            workflowTriggers = WorkflowTriggerService.getService().pause(WorkflowId.build(namespace, workflowName));
        }
        return Response.status(OK).entity(workflowTriggers).build();
    }

    @PUT
    @Path("/{name}")
    @ApiOperation(value = "Enable/Disable workflow trigger", response = WorkflowTrigger.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Affected workflow trigger. If empty it means workflow trigger was already " +
                    "in resultant state"),
            @ApiResponse(code = 404, message = "Workflow trigger not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateWorkflowTrigger(@ApiParam(value = "workflow name", required = true)
                                          @PathParam("workflow") String workflowName,
                                          @ApiParam(value = "workflow trigger name", required = true)
                                          @PathParam("name") String triggerName,
                                          @ApiParam(value = "enable/ disable the workflow trigger", required = true)
                                          @DefaultValue("true") @QueryParam("enable") boolean enable,
                                          @HeaderParam("namespace") String namespace)
            throws ServiceException, ValidationException, SchedulerException {
        logger.info("Received request to update workflow trigger {} for workflow {} under namespace {} set enable to {}",
                triggerName, workflowName, namespace);
        if (namespace == null || namespace.isEmpty()) {
            return Response.status(BAD_REQUEST).entity("missing namespace header").build();
        }
        final WorkflowTrigger workflowTrigger;
        if (enable) {
            workflowTrigger = WorkflowTriggerService.getService().resume(WorkflowTriggerId.build(namespace, triggerName, workflowName));
        } else {
            workflowTrigger = WorkflowTriggerService.getService().pause(WorkflowTriggerId.build(namespace, triggerName, workflowName));
        }
        return Response.status(OK).entity(workflowTrigger).build();
    }

    @DELETE
    @Path("/{name}")
    @ApiOperation(value = "Delete workflow trigger by name")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Workflow trigger not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteWorkflowTrigger(@ApiParam(value = "workflow name", required = true)
                                          @PathParam("workflow") String workflowName,
                                          @ApiParam(value = "workflow trigger name", required = true)
                                          @PathParam("name") String triggerName,
                                          @HeaderParam("namespace") String namespace) throws ServiceException, SchedulerException, ValidationException {
        logger.info("Received request to delete workflow trigger with name {} for workflow {} under namespace {}",
                triggerName, workflowName, namespace);
        if (namespace == null || namespace.isEmpty()) {
            return Response.status(BAD_REQUEST).entity("missing namespace header").build();
        }
        WorkflowTriggerId triggerId = WorkflowTriggerId.build(namespace, triggerName, workflowName);
        WorkflowTriggerService.getService().delete(triggerId);
        return Response.status(OK).build();
    }
}
