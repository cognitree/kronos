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

package com.cognitree.kronos;

import com.cognitree.kronos.model.Namespace;
import com.cognitree.kronos.model.Workflow;
import com.cognitree.kronos.model.WorkflowTrigger;
import com.cognitree.kronos.scheduler.NamespaceService;
import com.cognitree.kronos.scheduler.ServiceException;
import com.cognitree.kronos.scheduler.ValidationException;
import com.cognitree.kronos.scheduler.WorkflowService;
import com.cognitree.kronos.scheduler.WorkflowTriggerService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class FileReader {

    private static final Logger logger = LoggerFactory.getLogger(FileReader.class);

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private static final TypeReference<List<Namespace>> NAMESPACE_LIST_REF =
            new TypeReference<List<Namespace>>() {
            };
    private static final TypeReference<List<Workflow>> WORKFLOW_LIST_REF =
            new TypeReference<List<Workflow>>() {
            };
    private static final TypeReference<List<WorkflowTrigger>> WORKFLOW_TRIGGER_LIST_REF =
            new TypeReference<List<WorkflowTrigger>>() {
            };

    public void loadNamespaces() throws IOException, ServiceException {
        final InputStream resourceAsStream =
                FileReader.class.getClassLoader().getResourceAsStream("namespaces.yaml");
        List<Namespace> namespaces = MAPPER.readValue(resourceAsStream, NAMESPACE_LIST_REF);

        for (Namespace namespace : namespaces) {
            if (NamespaceService.getService().get(namespace.getIdentity()) == null) {
                try {
                    NamespaceService.getService().add(namespace);
                } catch (Exception ex) {
                    logger.error("Unable to add namespace {}", namespace, ex);
                }
            } else {
                logger.error("Namespace already exists with name {}", namespace.getIdentity());
            }
        }
    }

    public void loadWorkflows() throws IOException, ServiceException, ValidationException {
        final InputStream resourceAsStream =
                FileReader.class.getClassLoader().getResourceAsStream("workflows.yaml");
        List<Workflow> workflows = MAPPER.readValue(resourceAsStream, WORKFLOW_LIST_REF);

        for (Workflow workflow : workflows) {
            if (WorkflowService.getService().get(workflow) == null) {
                try {
                    WorkflowService.getService().add(workflow);
                } catch (Exception ex) {
                    logger.error("Unable to add workflow {}", workflow, ex);
                }
            } else {
                logger.error("Workflow already exists with id {}", workflow.getIdentity());
            }
        }
    }

    public void loadWorkflowTriggers() throws IOException, ServiceException, ValidationException {
        final InputStream resourceAsStream =
                FileReader.class.getClassLoader().getResourceAsStream("workflow-triggers.yaml");
        List<WorkflowTrigger> workflowTriggers = MAPPER.readValue(resourceAsStream, WORKFLOW_TRIGGER_LIST_REF);

        for (WorkflowTrigger workflowTrigger : workflowTriggers) {
            if (WorkflowTriggerService.getService().get(workflowTrigger) == null) {
                try {
                    WorkflowTriggerService.getService().add(workflowTrigger);
                } catch (Exception ex) {
                    logger.error("Unable to add workflow trigger {}", workflowTrigger, ex);
                }
            } else {
                logger.error("Workflow trigger already exists with id {}", workflowTrigger.getIdentity());
            }
        }
    }
}
