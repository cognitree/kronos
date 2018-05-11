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

package com.cognitree.tasks.scheduler.policies;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/**
 * defines configuration for a {@link TimeoutPolicy}
 */
public class TimeoutPolicyConfig {

    /**
     * fully qualified class name of the {@link TimeoutPolicy} implementation
     */
    private String policyClass;

    /**
     * Configuration to be passed to timeout policy to instantiate itself.
     * This will be passed as an arg to the {@link TimeoutPolicy#init(ObjectNode, TimeoutAction)}
     * method at the time of instantiation.
     */
    private ObjectNode config;

    public String getPolicyClass() {
        return policyClass;
    }

    public void setPolicyClass(String policyClass) {
        this.policyClass = policyClass;
    }

    public ObjectNode getConfig() {
        return config;
    }

    public void setConfig(ObjectNode config) {
        this.config = config;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TimeoutPolicyConfig)) return false;
        TimeoutPolicyConfig that = (TimeoutPolicyConfig) o;
        return Objects.equals(policyClass, that.policyClass) &&
                Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {

        return Objects.hash(policyClass, config);
    }

    @Override
    public String toString() {
        return "TimeoutPolicyConfig{" +
                "policyClass='" + policyClass + '\'' +
                ", config=" + config +
                '}';
    }
}
