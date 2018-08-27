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

package com.cognitree.kronos.model.definitions;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Objects;

@JsonSerialize(as = WorkflowTriggerId.class)
@JsonDeserialize(as = WorkflowTriggerId.class)
public class WorkflowTriggerId {

    private String name;
    private String workflowName;
    private String namespace;

    public static WorkflowTriggerId build(String name, String workflowName, String namespace) {
        final WorkflowTriggerId workflowDefinitionId = new WorkflowTriggerId();
        workflowDefinitionId.setName(name);
        workflowDefinitionId.setWorkflowName(workflowName);
        workflowDefinitionId.setNamespace(namespace);
        return workflowDefinitionId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkflowTriggerId)) return false;
        WorkflowTriggerId that = (WorkflowTriggerId) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(workflowName, that.workflowName) &&
                Objects.equals(namespace, that.namespace);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, workflowName, namespace);
    }

    @Override
    public String toString() {
        return "WorkflowTriggerId{" +
                "name='" + name + '\'' +
                ", workflowName='" + workflowName + '\'' +
                ", namespace='" + namespace + '\'' +
                '}';
    }
}
