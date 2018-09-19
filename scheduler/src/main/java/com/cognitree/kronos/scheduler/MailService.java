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

package com.cognitree.kronos.scheduler;

import com.cognitree.kronos.Service;
import com.cognitree.kronos.ServiceProvider;
import com.cognitree.kronos.model.Task;
import com.cognitree.kronos.model.TaskId;
import com.cognitree.kronos.scheduler.model.CalendarIntervalSchedule;
import com.cognitree.kronos.scheduler.model.CronSchedule;
import com.cognitree.kronos.scheduler.model.Job;
import com.cognitree.kronos.scheduler.model.JobId;
import com.cognitree.kronos.scheduler.model.Messages;
import com.cognitree.kronos.scheduler.model.Schedule;
import com.cognitree.kronos.scheduler.model.Workflow;
import com.cognitree.kronos.scheduler.model.WorkflowId;
import com.cognitree.kronos.scheduler.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.model.WorkflowTriggerId;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.simplejavamail.email.EmailBuilder;
import org.simplejavamail.email.EmailPopulatingBuilder;
import org.simplejavamail.mailer.Mailer;
import org.simplejavamail.mailer.MailerBuilder;
import org.simplejavamail.mailer.config.TransportStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.internet.InternetAddress;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static com.cognitree.kronos.scheduler.model.Job.Status.FAILED;
import static com.cognitree.kronos.scheduler.model.Job.Status.SUCCESSFUL;
import static com.cognitree.kronos.scheduler.model.Schedule.Type.calendar;
import static com.cognitree.kronos.scheduler.model.Schedule.Type.cron;

public class MailService implements Service {
    private static final Logger logger = LoggerFactory.getLogger(MailService.class);
    private final MailConfig mailConfig;
    private Mailer mailer;

    public MailService(MailConfig mailConfig) {
        this.mailConfig = mailConfig;
    }

    public static MailService getService() {
        return (MailService) ServiceProvider.getService(MailService.class.getSimpleName());
    }

    @Override
    public void init() {
        logger.info("Initializing mail service");
    }

    @Override
    public void start() {
        if (mailConfig != null) {
            mailer = MailerBuilder
                    .withSMTPServer(mailConfig.getSmtpHost(), mailConfig.getPort(),
                            mailConfig.getUserEmail(), mailConfig.getPassword())
                    .withTransportStrategy(TransportStrategy.SMTP_TLS)
                    .withSessionTimeout(10 * 1000)
                    .buildMailer();
            ServiceProvider.registerService(this);
        }
        TaskService.getService().registerListener(new TaskNotificationHandler());
        JobService.getService().registerListener(new JobNotificationHandler());
    }

    public void send(String subject, String body, List<String> recipients, boolean highPriority) {
        logger.trace("received request to send email with subject {}, body {} to recipients {}",
                subject, body, recipients);
        try {
            final EmailPopulatingBuilder emailBuilder = EmailBuilder
                    .startingBlank()
                    .from(new InternetAddress(mailConfig.getFromEmail(), mailConfig.getFromName()))
                    .toMultiple(recipients)
                    .withSubject(subject)
                    .withReplyTo(mailConfig.getReplyTo() != null ? mailConfig.getReplyTo() : mailConfig.getFromEmail())
                    .withHTMLText(body)
                    .withPlainText(body);

            if (highPriority) {
                emailBuilder.withHeader("X-Priority", 1);
            }
            mailer.sendMail(emailBuilder.buildEmail(), true);
        } catch (Exception e) {
            logger.error("error sending email with subject {}, body {} to recipients {}", subject, body, recipients, e);
        }
    }

    @Override
    public void stop() {
    }

    private static class JobNotificationHandler implements JobStatusChangeListener {

        private static final String JOB_SUBJECT_TEMPLATE_VM = "job-subject-template.vm";
        private static final String JOB_BODY_TEMPLATE_VM = "job-body-template.vm";
        private Template subjectTemplate;
        private Template bodyTemplate;

        public JobNotificationHandler() {
            VelocityEngine velocityEngine = new VelocityEngine();
            velocityEngine.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
            velocityEngine.setProperty("runtime.log.logsystem.class", "org.apache.velocity.runtime.log.NullLogChute");
            velocityEngine.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
            subjectTemplate = velocityEngine.getTemplate(JOB_SUBJECT_TEMPLATE_VM);
            bodyTemplate = velocityEngine.getTemplate(JOB_BODY_TEMPLATE_VM);
        }

        @Override
        public void statusChanged(JobId jobId, Job.Status from, Job.Status to) {
            if (to.isFinal()) {
                try {
                    Job job = JobService.getService().get(jobId);
                    if (job == null) {
                        logger.error("Received status change notification for job {}, which does not exists", jobId);
                        return;
                    }
                    notifyJobCompletion(job, to);
                } catch (ServiceException | ValidationException e) {
                    logger.error("Error handling job {} status change from {}, to {}", jobId, from, to);
                }
            }
        }

        private void notifyJobCompletion(Job job, Job.Status status) {
            final MailService mailService = MailService.getService();
            if (mailService == null) {
                logger.warn("unable to send job completion notification as mail service is not initialized");
                return;
            }
            try {
                final Workflow workflow = WorkflowService.getService().get(WorkflowId.build(job.getWorkflow(), job.getNamespace()));
                List<String> recipients = null;
                if (status == SUCCESSFUL) {
                    recipients = workflow.getEmailOnSuccess();
                }
                if (status == Job.Status.FAILED) {
                    recipients = workflow.getEmailOnFailure();
                }
                if (recipients != null && !recipients.isEmpty()) {
                    final List<Task> tasks = TaskService.getService().get(job.getId(), job.getWorkflow(), job.getNamespace());
                    VelocityContext velocityContext = buildVelocityContext(job, workflow, tasks);
                    StringWriter subjectWriter = new StringWriter();
                    subjectTemplate.merge(velocityContext, subjectWriter);
                    StringWriter bodyWriter = new StringWriter();
                    bodyTemplate.merge(velocityContext, bodyWriter);
                    mailService.send(subjectWriter.toString(), bodyWriter.toString(), recipients, status == FAILED);
                }
            } catch (Exception e) {
                logger.error("error sending email notification on completion for job {}", job, e);
            }
        }

        private VelocityContext buildVelocityContext(Job job, Workflow workflow, List<Task> tasks)
                throws ServiceException, ValidationException {
            VelocityContext velocityContext = new VelocityContext();
            velocityContext.put("job", job);
            velocityContext.put("workflow", workflow);
            velocityContext.put("tasks", tasks);
            final WorkflowTriggerId triggerId = WorkflowTriggerId.build(job.getTrigger(), job.getWorkflow(), job.getNamespace());
            final WorkflowTrigger workflowTrigger = WorkflowTriggerService.getService().get(triggerId);
            final Schedule schedule = workflowTrigger.getSchedule();
            TimeZone timeZone;
            if (schedule.getType() == cron && ((CronSchedule) schedule).getTimezone() != null) {
                timeZone = TimeZone.getTimeZone(((CronSchedule) schedule).getTimezone());
            } else if (schedule.getType() == calendar && ((CalendarIntervalSchedule) schedule).getTimezone() != null) {
                timeZone = TimeZone.getTimeZone(((CalendarIntervalSchedule) schedule).getTimezone());
            } else {
                timeZone = TimeZone.getDefault();
            }
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            dateFormat.setTimeZone(timeZone);
            velocityContext.put("date", new Date());
            velocityContext.put("dateFormat", dateFormat);
            return velocityContext;
        }
    }

    private static class TaskNotificationHandler implements TaskStatusChangeListener {

        private static final String TASK_SUBJECT_TEMPLATE_VM = "task-subject-template.vm";
        private static final String TASK_BODY_TEMPLATE_VM = "task-body-template.vm";
        private Template subjectTemplate;
        private Template bodyTemplate;

        public TaskNotificationHandler() {
            VelocityEngine velocityEngine = new VelocityEngine();
            velocityEngine.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
            velocityEngine.setProperty("runtime.log.logsystem.class", "org.apache.velocity.runtime.log.NullLogChute");
            velocityEngine.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
            subjectTemplate = velocityEngine.getTemplate(TASK_SUBJECT_TEMPLATE_VM);
            bodyTemplate = velocityEngine.getTemplate(TASK_BODY_TEMPLATE_VM);
        }

        @Override
        public void statusChanged(TaskId taskId, Task.Status from, Task.Status to) {
            if (to == Task.Status.FAILED) {
                try {
                    Task task = TaskService.getService().get(taskId);
                    // ignore if failure is due to task dependency resolution
                    if (!task.getStatusMessage().equals(Messages.FAILED_TO_RESOLVE_DEPENDENCY)) {
                        notifyTaskFailure(task);
                    }
                } catch (ServiceException e) {
                    logger.error("Error handling task {} status change from {}, to {}", taskId, from, to);
                }

            }
        }

        void notifyTaskFailure(Task task) {
            final MailService mailService = MailService.getService();
            if (mailService == null) {
                logger.warn("unable to send task failure notification as mail service is not initialized");
                return;
            }
            try {
                final Job job = JobService.getService().get(JobId.build(task.getJob(), task.getWorkflow(), task.getNamespace()));
                final Workflow workflow = WorkflowService.getService().get(WorkflowId.build(job.getWorkflow(), job.getNamespace()));
                List<String> recipients = workflow.getEmailOnFailure();
                if (recipients != null && !recipients.isEmpty()) {
                    VelocityContext velocityContext = buildVelocityContext(task, job, workflow);
                    StringWriter subjectWriter = new StringWriter();
                    subjectTemplate.merge(velocityContext, subjectWriter);
                    StringWriter bodyWriter = new StringWriter();
                    bodyTemplate.merge(velocityContext, bodyWriter);
                    mailService.send(subjectWriter.toString(), bodyWriter.toString(), recipients, true);
                }
            } catch (Exception e) {
                logger.error("error sending email notification on failure of task {}", task, e);
            }
        }

        private VelocityContext buildVelocityContext(Task task, Job job, Workflow workflow)
                throws ServiceException, ValidationException {
            VelocityContext velocityContext = new VelocityContext();
            velocityContext.put("job", job);
            velocityContext.put("task", task);
            velocityContext.put("workflow", workflow);
            final WorkflowTriggerId triggerId = WorkflowTriggerId.build(job.getTrigger(), job.getWorkflow(), job.getNamespace());
            final WorkflowTrigger workflowTrigger = WorkflowTriggerService.getService().get(triggerId);
            final Schedule schedule = workflowTrigger.getSchedule();
            TimeZone timeZone;
            if (schedule.getType() == cron && ((CronSchedule) schedule).getTimezone() != null) {
                timeZone = TimeZone.getTimeZone(((CronSchedule) schedule).getTimezone());
            } else if (schedule.getType() == calendar && ((CalendarIntervalSchedule) schedule).getTimezone() != null) {
                timeZone = TimeZone.getTimeZone(((CalendarIntervalSchedule) schedule).getTimezone());
            } else {
                timeZone = TimeZone.getDefault();
            }
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            dateFormat.setTimeZone(timeZone);
            velocityContext.put("date", new Date());
            velocityContext.put("dateFormat", dateFormat);
            return velocityContext;
        }
    }

}
