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
import com.cognitree.kronos.scheduler.WorkflowService;
import com.cognitree.kronos.scheduler.model.WorkflowStatistics;
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
import java.util.concurrent.TimeUnit;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.OK;

@Path("statistics/workflows")
@Api(value = "workflow statistics", description = "apis to query workflow statistics")
public class WorkflowStatisticsResource {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowStatisticsResource.class);
    private static final String DEFAULT_DAYS = "10";

    @GET
    @ApiOperation(value = "Get statistics across workflows", response = WorkflowStatistics.class,
            notes = "query param 'from' and 'to' takes precedence over 'date_range'. " +
                    "If 'from' is specified without 'to' it means get all jobs from 'from' timestamp till now." +
                    "If 'to' is specified without 'from' it means get all jobs from beginning till 'from' timestamp")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllWorkflowStatistics(@ApiParam(value = "Start time of the range")
                                             @QueryParam("from") long createdAfter,
                                             @ApiParam(value = "End time of the range")
                                             @QueryParam("to") long createdBefore,
                                             @ApiParam(value = "Number of days to fetch jobs from today", defaultValue = "10")
                                             @DefaultValue(DEFAULT_DAYS) @QueryParam("date_range") int numberOfDays,
                                             @HeaderParam("namespace") String namespace) throws ServiceException, ValidationException {
        logger.info("Received request to get stats across workflow under namespace {} with param createdAfter {}, " +
                "createdBefore {}, numberOfDays {}", namespace, createdAfter, createdBefore, numberOfDays);
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

        final WorkflowStatistics workflowStatistics =
                WorkflowService.getService().getStatistics(namespace, createdAfter, createdBefore);
        return Response.status(OK).entity(workflowStatistics).build();
    }

    @GET
    @Path("{name}")
    @ApiOperation(value = "Get statistics for workflow by name", response = WorkflowStatistics.class,
            notes = "query param 'from' and 'to' takes precedence over 'date_range'. " +
                    "If 'from' is specified without 'to' it means get all jobs from 'from' timestamp till now." +
                    "If 'to' is specified without 'from' it means get all jobs from beginning till 'from' timestamp")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Workflow not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflowStatistics(@ApiParam(value = "workflow name", required = true)
                                          @PathParam("name") String name,
                                          @ApiParam(value = "Start time of the range")
                                          @QueryParam("from") long createdAfter,
                                          @ApiParam(value = "End time of the range")
                                          @QueryParam("to") long createdBefore,
                                          @ApiParam(value = "Number of days to fetch jobs from today", defaultValue = "10")
                                          @DefaultValue(DEFAULT_DAYS) @QueryParam("date_range") int numberOfDays,
                                          @HeaderParam("namespace") String namespace) throws ServiceException, ValidationException {
        logger.info("Received request to get stats for workflow with name {} under namespace {} with param " +
                "createdAfter {}, createdBefore {}, numberOfDays {}", name, namespace, createdAfter, createdBefore, numberOfDays);
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

        final WorkflowStatistics workflowStatistics =
                WorkflowService.getService().getStatistics(namespace, name, createdAfter, createdBefore);
        return Response.status(OK).entity(workflowStatistics).build();
    }

    private long timeInMillisBeforeDays(int numberOfDays) {
        final long currentTimeMillis = System.currentTimeMillis();
        return numberOfDays == -1 ? 0 : currentTimeMillis - (currentTimeMillis % TimeUnit.DAYS.toMillis(1))
                - TimeUnit.DAYS.toMillis(numberOfDays - 1);
    }
}
