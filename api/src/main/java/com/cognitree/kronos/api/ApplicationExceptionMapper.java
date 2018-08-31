package com.cognitree.kronos.api;

import com.cognitree.kronos.scheduler.ValidationException;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

public class ApplicationExceptionMapper<T extends Exception> implements ExceptionMapper<T> {
    @Override
    public Response toResponse(T exception) {
        Response.Status status;
        if (exception instanceof ValidationException) {
            status = BAD_REQUEST;
        } else {
            status = INTERNAL_SERVER_ERROR;
        }
        return Response.status(status).entity(exception.getMessage()).build();
    }
}
