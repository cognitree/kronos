package com.cognitree.kronos.webserver.api;

import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.WorkflowId;
import com.cognitree.kronos.model.definitions.WorkflowDefinition.WorkflowTask;
import com.cognitree.kronos.scheduler.store.WorkflowStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

@Path("workflows")
public class WorkflowResource {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowResource.class);

    private static final String DEFAULT_NAMESPACE = "default";
    private static final String DEFAULT_DAYS = "10";

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllWorkflow(@QueryParam("name") String workflowName,
                                   @DefaultValue(DEFAULT_DAYS) @QueryParam("date_range") int numberOfDays) {
        final List<Workflow> workflows;
        final long currentTimeMillis = System.currentTimeMillis();
        long createdAfter = currentTimeMillis - (currentTimeMillis % TimeUnit.DAYS.toMillis(1))
                - TimeUnit.DAYS.toMillis(numberOfDays - 1);
        long createdBefore = createdAfter + TimeUnit.DAYS.toMillis(numberOfDays);
        if (workflowName == null) {
            logger.info("Received request to get all workflow with date range {}", numberOfDays);
            workflows = WorkflowStoreService.getService().load(createdAfter, createdBefore);
        } else {
            logger.info("Received request to get all workflow with name {}, date range {}", workflowName, numberOfDays);
            workflows = WorkflowStoreService.getService().loadByName(workflowName, DEFAULT_NAMESPACE, createdAfter, createdBefore);
        }
        return Response.status(OK).entity(workflows).build();
    }

    @GET
    @Path("{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflow(@PathParam("id") String id) {
        logger.info("Received request to get workflow with id {}", id);
        WorkflowId workflowId = WorkflowId.create(id, DEFAULT_NAMESPACE);
        final Workflow workflow = WorkflowStoreService.getService().load(workflowId);
        if (workflow == null) {
            logger.error("No workflow exists with id {}", id);
            return Response.status(NOT_FOUND).build();
        }
        return Response.status(OK).entity(workflow).build();
    }

    @GET
    @Path("{id}/tasks")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflowTasks(@PathParam("id") String id) {
        WorkflowId workflowId = WorkflowId.create(id, DEFAULT_NAMESPACE);
        final Workflow workflow = WorkflowStoreService.getService().load(workflowId);
        if (workflow == null) {
            logger.error("No workflow exists with id {}", workflowId);
            return Response.status(NOT_FOUND).build();
        }
        final List<Task> workflowTasks =
                WorkflowStoreService.getService().getWorkflowTasks(workflow);
        return Response.ok(workflowTasks).build();
    }

    @GET
    @Path("{id}/taskdefs")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWorkflowTaskDefs(@PathParam("id") String id) {
        WorkflowId workflowId = WorkflowId.create(id, DEFAULT_NAMESPACE);
        final Workflow workflow = WorkflowStoreService.getService().load(workflowId);
        if (workflow == null) {
            logger.error("No workflow exists with id {}", workflowId);
            return Response.status(NOT_FOUND).build();
        }
        final List<WorkflowTask> workflowTaskDefs =
                WorkflowStoreService.getService().getWorkflowTaskDefs(workflow);
        return Response.ok(workflowTaskDefs).build();
    }
}
