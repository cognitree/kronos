package com.cognitree.kronos.api;

import com.cognitree.kronos.scheduler.ServiceException;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

public class ServiceExceptionMapper implements ExceptionMapper<ServiceException> {
    @Override
    public Response toResponse(ServiceException exception) {
        return Response.status(INTERNAL_SERVER_ERROR).entity(exception.getMessage()).build();
    }
}
