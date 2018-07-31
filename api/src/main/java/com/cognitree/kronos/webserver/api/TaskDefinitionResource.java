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

package com.cognitree.kronos.webserver.api;

import com.cognitree.kronos.model.definitions.TaskDefinition;
import com.cognitree.kronos.model.definitions.TaskDefinitionId;
import com.cognitree.kronos.scheduler.store.TaskDefinitionStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

import static javax.ws.rs.core.Response.Status.*;

@Path("taskdefs")
public class TaskDefinitionResource {
    private static final Logger logger = LoggerFactory.getLogger(TaskDefinitionResource.class);

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllTaskDefinition() {
        logger.info("Received request to get all task definitions");
        final List<TaskDefinition> taskDefinitions = TaskDefinitionStoreService.getService().load();
        return Response.status(OK).entity(taskDefinitions).build();
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTaskDefinition(@PathParam("name") String name) {
        logger.info("Received request to get task task definition with name {}", name);
        TaskDefinitionId taskDefinitionId = TaskDefinitionId.create(name);
        final TaskDefinition taskDefinition = TaskDefinitionStoreService.getService().load(taskDefinitionId);
        if (taskDefinition == null) {
            logger.error("No task definition found with name {}", name);
            return Response.status(NOT_FOUND).build();
        }
        return Response.status(OK).entity(taskDefinition).build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response addTaskDefinition(TaskDefinition taskDefinition) {
        logger.info("Received request to add task definition {}", taskDefinition);
        if (TaskDefinitionStoreService.getService().load(taskDefinition) != null) {
            logger.error("Task definition already exists with name {}", taskDefinition.getName());
            return Response.status(CONFLICT).build();
        }
        TaskDefinitionStoreService.getService().store(taskDefinition);
        return Response.status(CREATED).entity(taskDefinition).build();
    }

    @PUT
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateTaskDefinition(@PathParam("name") String name, TaskDefinition taskDefinition) {
        taskDefinition.setName(name);
        logger.info("Received request to update task definition with name {} to {}", name, taskDefinition);
        if (TaskDefinitionStoreService.getService().load(taskDefinition) == null) {
            logger.error("No task definition exists with name {}", name);
            return Response.status(NOT_FOUND).build();
        }
        TaskDefinitionStoreService.getService().update(taskDefinition);
        return Response.status(OK).entity(taskDefinition).build();
    }

    @DELETE
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteTaskDefinition(@PathParam("name") String name) {
        logger.info("Received request to delete task definition with name {}", name);
        TaskDefinitionId taskDefinitionId = TaskDefinitionId.create(name);
        if (TaskDefinitionStoreService.getService().load(taskDefinitionId) == null) {
            logger.error("No task definition exists with name {}", name);
            return Response.status(NOT_FOUND).build();
        }
        TaskDefinitionStoreService.getService().delete(taskDefinitionId);
        return Response.status(OK).build();
    }
}
