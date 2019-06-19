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

import com.cognitree.kronos.scheduler.NamespaceService;
import com.cognitree.kronos.ServiceException;
import com.cognitree.kronos.scheduler.ValidationException;
import com.cognitree.kronos.scheduler.model.Namespace;
import com.cognitree.kronos.scheduler.model.NamespaceId;
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

import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.NOT_IMPLEMENTED;
import static javax.ws.rs.core.Response.Status.OK;

@Path("/namespaces")
@Api(value = "namespaces", description = "manage namespaces")
public class NamespaceResource {
    private static final Logger logger = LoggerFactory.getLogger(NamespaceResource.class);

    @GET
    @ApiOperation(value = "Get all namespaces", response = Namespace.class, responseContainer = "List")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllNamespaces() throws ServiceException {
        logger.info("Received request to get all namespaces");
        final List<Namespace> namespaces = NamespaceService.getService().get();
        return Response.status(OK).entity(namespaces).build();
    }

    @GET
    @Path("{name}")
    @ApiOperation(value = "Get namespace by name", response = Namespace.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Namespace not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response getNamespace(@ApiParam(value = "namespace name", required = true)
                                 @PathParam("name") String name) throws ServiceException {
        logger.info("Received request to get namespace with name {}", name);
        final Namespace namespace = NamespaceService.getService().get(NamespaceId.build(name));
        if (namespace == null) {
            logger.error("No namespace found with name {}", name);
            return Response.status(NOT_FOUND).build();
        }
        return Response.status(OK).entity(namespace).build();
    }

    @POST
    @ApiOperation(value = "Add new namespace", response = Namespace.class)
    @ApiResponses(value = {
            @ApiResponse(code = 409, message = "Namespace already exists")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response addNamespace(Namespace namespace) throws ServiceException, ValidationException {
        logger.info("Received request to add namespace {}", namespace);
        NamespaceService.getService().add(namespace);
        return Response.status(CREATED).entity(namespace).build();
    }

    @PUT
    @Path("{name}")
    @ApiOperation(value = "Update namespace", response = Namespace.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Namespace not found")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateNamespace(@ApiParam(value = "namespace name", required = true)
                                    @PathParam("name") String name,
                                    Namespace namespace) throws ServiceException, ValidationException {
        namespace.setName(name);
        logger.info("Received request to update namespace with name {} to {}", name, namespace);
        NamespaceService.getService().update(namespace);
        return Response.status(OK).entity(namespace).build();
    }

    @DELETE
    @Path("{name}")
    @ApiOperation(value = "Delete namespace (not implemented)")
    @ApiResponses(value = {
            @ApiResponse(code = 501, message = "not implemented")})
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteNamespace(@ApiParam(value = "namespace name", required = true)
                                    @PathParam("name") String name) {
        logger.info("Received request to delete namespace with name {}", name);
        return Response.status(NOT_IMPLEMENTED).build();
    }
}
