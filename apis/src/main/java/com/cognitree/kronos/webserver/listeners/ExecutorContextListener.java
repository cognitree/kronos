package com.cognitree.kronos.webserver.listeners;

import com.cognitree.kronos.executor.ExecutorApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class ExecutorContextListener implements ServletContextListener {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorContextListener.class);
    private ExecutorApp executorApp;

    public ExecutorContextListener() {
        executorApp = new ExecutorApp();
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        try {
            logger.info("Starting executor");
            executorApp.start();
        } catch (Exception e) {
            logger.error("Error starting executor", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        logger.info("Stopping executor");
        executorApp.stop();
    }
}
