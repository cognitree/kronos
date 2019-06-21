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

import com.cognitree.kronos.scheduler.store.StoreService;
import com.cognitree.kronos.scheduler.store.StoreServiceConfig;

import java.util.Objects;

/**
 * defines configurations for scheduler.
 */
public class SchedulerConfig {

    /**
     * {@link StoreService} configuration, required by the scheduler to instantiate the store service.
     */
    private StoreServiceConfig storeServiceConfig;

    /**
     * configuration required by {@link MailService} to configure itself
     */
    private MailConfig mailConfig;

    /**
     * enable {@link ConfigurationService} that consumes config updates from a specified queue and processes them
     */
    private boolean enableConfigurationService = false;

    public StoreServiceConfig getStoreServiceConfig() {
        return storeServiceConfig;
    }

    public void setStoreServiceConfig(StoreServiceConfig storeServiceConfig) {
        this.storeServiceConfig = storeServiceConfig;
    }

    public MailConfig getMailConfig() {
        return mailConfig;
    }

    public void setMailConfig(MailConfig mailConfig) {
        this.mailConfig = mailConfig;
    }

    public boolean isEnableConfigurationService() {
        return enableConfigurationService;
    }

    public void setEnableConfigurationService(boolean enableConfigurationService) {
        this.enableConfigurationService = enableConfigurationService;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SchedulerConfig)) return false;
        SchedulerConfig that = (SchedulerConfig) o;
        return enableConfigurationService == that.enableConfigurationService &&
                Objects.equals(storeServiceConfig, that.storeServiceConfig) &&
                Objects.equals(mailConfig, that.mailConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storeServiceConfig, mailConfig, enableConfigurationService);
    }

    @Override
    public String toString() {
        return "SchedulerConfig{" +
                "storeServiceConfig=" + storeServiceConfig +
                ", mailConfig=" + mailConfig +
                ", enableConfigurationService=" + enableConfigurationService +
                '}';
    }
}
