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

import com.cognitree.kronos.scheduler.JobService;
import com.cognitree.kronos.scheduler.ServiceException;
import com.cognitree.kronos.scheduler.ValidationException;
import com.cognitree.kronos.scheduler.model.Job;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.cognitree.kronos.scheduler.model.Job.Status;
import static java.util.Comparator.comparing;
import static javax.ws.rs.core.Response.Status.OK;

@Path("/jobs")
@Api(value = "jobs", description = "manage runtime instance of workflow - jobs")
public class JobResource {
    private static final Logger logger = LoggerFactory.getLogger(JobResource.class);
    private static final String DEFAULT_DAYS = "10";

    @GET
    @ApiOperation(value = "Get all running or executed jobs", response = Job.class, responseContainer = "List")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllJobs(@ApiParam(value = "job status", allowMultiple = true)
                               @QueryParam("status") List<Status> statuses,
                               @ApiParam(value = "Start time of the range")
                               @DefaultValue("-1") @QueryParam("from") long createdAfter,
                               @ApiParam(value = "End time of the range")
                               @DefaultValue("-1") @QueryParam("to") long createdBefore,
                               @ApiParam(value = "Number of days to fetch jobs from today", defaultValue = "10")
                               @DefaultValue(DEFAULT_DAYS) @QueryParam("date_range") int numberOfDays,
                               @HeaderParam("namespace") String namespace) throws ServiceException, ValidationException {
        logger.info("Received request to get all jobs under namespace {} with param status in {}, date range {}, " +
                "from {}, to {}", namespace, statuses, numberOfDays, createdAfter, createdBefore, namespace);

        if (createdAfter < 0 && createdBefore < 0) {
            createdAfter = timeInMillisBeforeDays(numberOfDays);
            createdBefore = System.currentTimeMillis();
        } else if (createdBefore > 0 && createdAfter < 0) {
            createdAfter = 0;
        } else if (createdBefore < 0) {
            createdBefore = System.currentTimeMillis();
        }

        final List<Job> jobs;
        if (statuses != null && !statuses.isEmpty()) {
            jobs = JobService.getService().get(statuses, namespace, createdAfter, createdBefore);
        } else {
            jobs = JobService.getService().get(namespace, createdAfter, createdBefore);
        }
        return Response.status(OK).entity(jobs.stream().sorted(comparing(Job::getCreatedAt).reversed())
                .collect(Collectors.toList())).build();
    }

    private long timeInMillisBeforeDays(int numberOfDays) {
        final long currentTimeMillis = System.currentTimeMillis();
        return numberOfDays == -1 ? 0 : currentTimeMillis - (currentTimeMillis % TimeUnit.DAYS.toMillis(1))
                - TimeUnit.DAYS.toMillis(numberOfDays - 1);
    }
}
