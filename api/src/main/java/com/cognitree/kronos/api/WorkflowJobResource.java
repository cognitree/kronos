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
import com.cognitree.kronos.response.JobResponse;
import com.cognitree.kronos.scheduler.JobService;
import com.cognitree.kronos.scheduler.ValidationException;
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.Job.Status;
import com.cognitree.kronos.scheduler.model.JobId;
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
import javax.ws.rs.POST;
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
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

@Path("/workflows/{workflow}/jobs")
@Api(value = "jobs", description = "manage runtime instance for a workflow - jobs")
public class WorkflowJobResource {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowJobResource.class);
    private static final String DEFAULT_DAYS = "10";

    @GET
    @ApiOperation(value = "Get all running or executed jobs for a workflow", response = Job.class, responseContainer = "List",
            notes = "query param 'from' and 'to' takes precedence over 'date_range'. " +
                    "If 'from' is specified without 'to' it means get all jobs from 'from' timestamp till now." +
                    "If 'to' is specified without 'from' it means get all jobs from beginning till 'from' timestamp")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllJobs(@ApiParam(value = "workflow name", required = true)
                               @PathParam("workflow") String workflowName,
                               @ApiParam(value = "workflow trigger name")
                               @QueryParam("trigger") String triggerName,
                               @ApiParam(value = "job status", allowMultiple = true)
                               @QueryParam("status") List<Status> statuses,
                               @ApiParam(value = "Start time of the range")
                               @QueryParam("from") long createdAfter,
                               @ApiParam(value = "End time of the range")
                               @QueryParam("to") long createdBefore,
                               @ApiParam(value = "Number of days to fetch jobs from today", defaultValue = "10")
                               @DefaultValue(DEFAULT_DAYS) @QueryParam("date_range") int numberOfDays,
                               @HeaderParam("namespace") String namespace) throws ServiceException, ValidationException {
        logger.info("Received request to get all jobs for workflow {} with param trigger name {}, status in {}" +
                        ", date range {}, from {}, to {}, namespace {}",
                workflowName, triggerName, statuses, numberOfDays, createdAfter, createdBefore, namespace);
        if (namespace == null || namespace.isEmpty()) {
            return Response.status(BAD_REQUEST).entity("missing namespace header").build();
        }
        if (createdAfter <= 0 && createdBefore <= 0) {
            createdAfter = timeInMillisBeforeDays(numberOfDays);
            createdBefore = System.currentTimeMillis();
        } else if (createdBefore > 0 && createdAfter <= 0) {
            createdAfter = 0;
        } else if (createdBefore <= 0) {
            createdBefore = System.currentTimeMillis();
        }

        final List<Job> jobs;
        if (triggerName != null) {
            if (statuses != null && !statuses.isEmpty()) {
                jobs = JobService.getService().get(namespace, workflowName, triggerName, statuses, createdAfter, createdBefore);
            } else {
                jobs = JobService.getService().get(namespace, workflowName, triggerName, createdAfter, createdBefore);
            }
        } else {
            if (statuses != null && !statuses.isEmpty()) {
                jobs = JobService.getService().get(namespace, workflowName, statuses, createdAfter, createdBefore);
            } else {
                jobs = JobService.getService().get(namespace, workflowName, createdAfter, createdBefore);

            }
        }
        return Response.status(OK).entity(jobs.stream().sorted(comparing(Job::getCreatedAt).reversed())
                .collect(Collectors.toList())).build();
    }

    @GET
    @Path("{id}")
    @ApiOperation(value = "Get job by id for a workflow", response = JobResponse.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Job not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJob(@ApiParam(value = "workflow name", required = true)
                           @PathParam("workflow") String workflowName,
                           @ApiParam(value = "job id", required = true)
                           @PathParam("id") String id,
                           @HeaderParam("namespace") String namespace) throws ServiceException, ValidationException {
        logger.info("Received request to get job with id {} under namespace {}", id, namespace);
        if (namespace == null || namespace.isEmpty()) {
            return Response.status(BAD_REQUEST).entity("missing namespace header").build();
        }
        final JobId jobId = JobId.build(namespace, id, workflowName);
        final Job job = JobService.getService().get(jobId);
        if (job == null) {
            logger.error("No job exists with id {}", jobId);
            return Response.status(NOT_FOUND).build();
        }
        final List<Task> tasks = JobService.getService().getTasks(job);
        return Response.status(OK).entity(JobResponse.create(job, tasks)).build();
    }

    @POST
    @Path("/{id}")
    @ApiOperation(value = "Execute an action on a job. Supported actions - abort")
    @Produces(MediaType.APPLICATION_JSON)
    public Response performAction(@ApiParam(value = "workflow name", required = true)
                                  @PathParam("workflow") String workflow,
                                  @ApiParam(value = "job id", required = true)
                                  @PathParam("id") String job,
                                  @ApiParam(value = "action", allowableValues = "abort")
                                  @QueryParam("action") String action,
                                  @HeaderParam("namespace") String namespace) throws ServiceException, ValidationException {
        logger.info("Received request to perform action {} on job {}, workflow {}",
                action, job, workflow);
        if (action.equals("abort")) {
            JobService.getService().abortJob(Job.build(namespace, job, workflow));
            return Response.status(Response.Status.OK).build();
        } else {
            return Response.status(Response.Status.BAD_REQUEST).entity("invalid action " + action).build();
        }
    }


    private long timeInMillisBeforeDays(int numberOfDays) {
        final long currentTimeMillis = System.currentTimeMillis();
        return numberOfDays == -1 ? 0 : currentTimeMillis - (currentTimeMillis % TimeUnit.DAYS.toMillis(1))
                - TimeUnit.DAYS.toMillis(numberOfDays - 1);
    }
}
