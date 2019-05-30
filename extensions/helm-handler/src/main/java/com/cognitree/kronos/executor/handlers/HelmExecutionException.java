package com.cognitree.kronos.executor.handlers;

public class HelmExecutionException extends Exception {

    public HelmExecutionException(String message) {
        super(message);
    }

    public HelmExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
