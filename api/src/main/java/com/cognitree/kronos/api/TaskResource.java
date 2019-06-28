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
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.scheduler.TaskService;
import com.cognitree.kronos.scheduler.ValidationException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/workflows/{workflow}/jobs/{job}/tasks")
@Api(value = "tasks", description = "manage runtime instance of workflow - tasks")
public class TaskResource {
    private static final Logger logger = LoggerFactory.getLogger(TaskResource.class);
    private static final String ABORT_ACTION = "abort";

    @POST
    @Path("/{task}")
    @ApiOperation(value = "Execute an action on a task. Supported actions - abort")
    @Produces(MediaType.APPLICATION_JSON)
    public Response performAction(@ApiParam(value = "workflow name", required = true)
                                  @PathParam("workflow") String workflow,
                                  @ApiParam(value = "job id", required = true)
                                  @PathParam("job") String job,
                                  @ApiParam(value = "task name", required = true)
                                  @PathParam("task") String task,
                                  @ApiParam(value = "action", allowableValues = ABORT_ACTION, required = true)
                                  @QueryParam("action") String action,
                                  @HeaderParam("namespace") String namespace) throws ServiceException, ValidationException {
        logger.info("Received request to perform action {} on task {} part of job {}, workflow {}",
                action, task, job, workflow);
        if (ABORT_ACTION.equals(action)) {
            TaskService.getService().abortTask(Task.build(namespace, task, job, workflow));
            return Response.status(Response.Status.OK).build();
        } else {
            return Response.status(Response.Status.BAD_REQUEST).entity("invalid action " + action).build();
        }
    }

}
