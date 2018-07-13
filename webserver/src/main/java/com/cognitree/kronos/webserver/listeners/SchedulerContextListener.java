package com.cognitree.kronos.webserver.listeners;

import com.cognitree.kronos.executor.ExecutorApp;
import com.cognitree.kronos.scheduler.SchedulerApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class SchedulerContextListener implements ServletContextListener {
    private static final Logger logger = LoggerFactory.getLogger(SchedulerContextListener.class);
    private SchedulerApp schedulerApp;
    private ExecutorApp executorApp;

    public SchedulerContextListener() {
        schedulerApp = new SchedulerApp();
        executorApp = new ExecutorApp();
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        try {
            logger.info("Starting scheduler");
            schedulerApp.start();
        } catch (Exception e) {
            logger.error("Error starting scheduler", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        logger.info("Stopping scheduler");
        schedulerApp.stop();
        executorApp.stop();
    }
}
