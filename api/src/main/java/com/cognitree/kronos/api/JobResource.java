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

import com.cognitree.kronos.model.Job;
import com.cognitree.kronos.model.JobId;
import com.cognitree.kronos.model.NamespaceId;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.response.JobResponse;
import com.cognitree.kronos.scheduler.JobService;
import com.cognitree.kronos.scheduler.NamespaceService;
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

@Path("/jobs")
@Api(value = "jobs", description = "manage runtime instance of workflow - jobs")
public class JobResource {
    private static final Logger logger = LoggerFactory.getLogger(JobResource.class);
    private static final String DEFAULT_DAYS = "10";

    @GET
    @ApiOperation(value = "Get all running or executed jobs", response = Job.class, responseContainer = "List")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllJobs(@ApiParam(value = "workflow name")
                               @QueryParam("workflow") String workflowName,
                               @ApiParam(value = "workflow trigger name")
                               @QueryParam("trigger") String triggerName,
                               @ApiParam(value = "Number of days to fetch jobs from today", defaultValue = "10")
                               @DefaultValue(DEFAULT_DAYS) @QueryParam("date_range") int numberOfDays,
                               @HeaderParam("namespace") String namespace) {
        if (!validateNamespace(namespace)) {
            return Response.status(BAD_REQUEST).entity("no namespace exists with name " + namespace).build();
        }

        if (triggerName != null && workflowName == null) {
            return Response.status(BAD_REQUEST)
                    .entity("workflow name is mandatory if trigger name is specified").build();
        }

        final List<Job> jobs;
        if (triggerName != null) {
            logger.info("Received request to get all jobs for workflow {} submitted by trigger {}" +
                            " in date range {} under namespace {}",
                    workflowName, triggerName, numberOfDays, namespace);
            jobs = JobService.getService().get(workflowName, triggerName, namespace, numberOfDays);
        } else if (workflowName != null) {
            logger.info("Received request to get all jobs for workflow {} in date range {} under namespace {}",
                    workflowName, numberOfDays, namespace);
            jobs = JobService.getService().get(workflowName, namespace, numberOfDays);
        } else {
            logger.info("Received request to get all jobs in date range {} under namespace {}",
                    numberOfDays, namespace);
            jobs = JobService.getService().get(namespace, numberOfDays);
        }
        return Response.status(OK).entity(jobs.stream().sorted(comparing(Job::getCreatedAt).reversed())
                .collect(Collectors.toList())).build();
    }

    @GET
    @Path("{id}")
    @ApiOperation(value = "Get job with id", response = JobResponse.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Job not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJob(@ApiParam(value = "job id", required = true)
                           @PathParam("id") String id,
                           @HeaderParam("namespace") String namespace) {
        logger.info("Received request to get job with id {} under namespace {}", id, namespace);
        if (!validateNamespace(namespace)) {
            return Response.status(BAD_REQUEST).entity("no namespace exists with name " + namespace).build();
        }
        JobId jobId = JobId.build(id, namespace);
        final Job job = JobService.getService().get(jobId);
        if (job == null) {
            logger.error("No job exists with id {} under namespace {}", id, namespace);
            return Response.status(NOT_FOUND).build();
        }
        final List<Task> tasks = JobService.getService().getTasks(job);
        return Response.status(OK).entity(JobResponse.create(job, tasks)).build();
    }

    private boolean validateNamespace(String name) {
        return name != null && NamespaceService.getService().get(NamespaceId.build(name)) != null;
    }
}
