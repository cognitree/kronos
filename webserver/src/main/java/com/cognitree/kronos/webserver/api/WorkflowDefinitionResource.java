package com.cognitree.kronos.webserver.api;

import com.cognitree.kronos.model.definitions.WorkflowDefinition;
import com.cognitree.kronos.model.definitions.WorkflowDefinitionId;
import com.cognitree.kronos.scheduler.WorkflowSchedulerService;
import com.cognitree.kronos.scheduler.store.WorkflowDefinitionStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

import static javax.ws.rs.core.Response.Status.*;

@Path("workflowdefs")
public class WorkflowDefinitionResource {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowDefinitionResource.class);

    private static final String DEFAULT_NAMESPACE = "default";

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllWorkflowDefinitions() {
        logger.info("Received request to get all workflow definitions");
        final List<WorkflowDefinition> workflowDefinitions = WorkflowDefinitionStoreService.getService().load();
        return Response.status(OK).entity(workflowDefinitions).build();
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflowDefinition(@PathParam("name") String workflowDefName) {
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
    @Produces(MediaType.APPLICATION_JSON)
    public Response addWorkflowDefinition(WorkflowDefinition workflowDefinition) {
        logger.info("Received request to add workflow definition {}", workflowDefinition);
        if (WorkflowDefinitionStoreService.getService().load(workflowDefinition) != null) {
            logger.error("Workflow definition already exists with name {}", workflowDefinition.getName());
            return Response.status(CONFLICT).build();
        }
        // override namespace of workflow
        workflowDefinition.setNamespace(DEFAULT_NAMESPACE);
        WorkflowSchedulerService.getService().isValid(workflowDefinition);
        WorkflowDefinitionStoreService.getService().store(workflowDefinition);
        WorkflowSchedulerService.getService().schedule(workflowDefinition);
        return Response.status(OK).entity(workflowDefinition).build();
    }

    @PUT
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateWorkflowDefinition(@PathParam("name") String workflowDefName,
                                             WorkflowDefinition workflowDefinition) {
        logger.info("Received request to update workflow definition with name {} to {}",
                workflowDefName, workflowDefinition);
        workflowDefinition.setName(workflowDefName);
        workflowDefinition.setNamespace(DEFAULT_NAMESPACE);
        if (WorkflowDefinitionStoreService.getService().load(workflowDefinition) == null) {
            logger.error("No workflow definition exists with name {}", workflowDefName);
            return Response.status(NOT_FOUND).build();
        }

        WorkflowSchedulerService.getService().isValid(workflowDefinition);
        WorkflowDefinitionStoreService.getService().update(workflowDefinition);
        WorkflowSchedulerService.getService().update(workflowDefinition);
        return Response.status(OK).entity(workflowDefinition).build();
    }

    @DELETE
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteWorkflowDefinition(@PathParam("name") String workflowDefName) {
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
