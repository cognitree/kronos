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

package com.cognitree.examples.tasks.scheduler.policies;

import com.cognitree.tasks.model.Task;
import com.cognitree.tasks.scheduler.policies.TimeoutAction;
import com.cognitree.tasks.scheduler.policies.TimeoutPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.simplejavamail.email.Email;
import org.simplejavamail.mailer.Mailer;
import org.simplejavamail.mailer.config.ServerConfig;
import org.simplejavamail.mailer.config.TransportStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.cognitree.tasks.model.FailureMessage.TIMED_OUT;
import static com.cognitree.tasks.model.Task.Status.FAILED;

public class NotifyOnTimeoutPolicy implements TimeoutPolicy {
    private static final Logger logger = LoggerFactory.getLogger(NotifyOnTimeoutPolicy.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private Mailer mailer;
    private String senderName;
    private String senderEmail;
    private String recipientName;
    private String recipientEmail;
    private String subject;
    private TimeoutAction timeoutAction;

    @Override
    public void init(ObjectNode policyConfig, TimeoutAction timeoutAction) {
        senderName = policyConfig.get("sender.name").asText();
        senderEmail = policyConfig.get("sender.email").asText();
        recipientName = policyConfig.get("recipient.name").asText();
        recipientEmail = policyConfig.get("recipient.email").asText();
        subject = policyConfig.get("email.subject").asText();
        final ServerConfig serverConfig =
                new ServerConfig(policyConfig.get("smtp.host").asText(), policyConfig.get("smtp.port").asInt(),
                        senderEmail, policyConfig.get("sender.password").asText());
        mailer = new Mailer(serverConfig, TransportStrategy.SMTP_TLS);
        this.timeoutAction = timeoutAction;
    }

    @Override
    public void handle(Task task) {
        try {
            Email email = new Email();
            email.setFromAddress(senderName, senderEmail);
            email.addNamedToRecipients(recipientName, recipientEmail);
            email.setSubject(subject);
            email.setText("Task : [" + OBJECT_MAPPER.writeValueAsString(task) + "] has timed out");
            email.setUseReturnReceiptTo(false);
            mailer.sendMail(email);
            timeoutAction.updateTask(task, FAILED, TIMED_OUT);
        } catch (Exception e) {
            logger.error("Error sending message for task {}", task, e);
        }

    }
}
