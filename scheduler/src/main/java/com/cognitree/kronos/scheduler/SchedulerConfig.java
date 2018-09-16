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

import com.cognitree.kronos.scheduler.store.StoreProvider;
import com.cognitree.kronos.scheduler.store.StoreProviderConfig;

import java.util.Objects;

/**
 * defines configurations for scheduler.
 */
public class SchedulerConfig {

    /**
     * {@link StoreProvider} configuration, required by the scheduler to instantiate the store provider.
     */
    private StoreProviderConfig storeProviderConfig;


    /**
     * configuration required by {@link MailService} to configure itself
     */
    private MailConfig mailConfig;

    public StoreProviderConfig getStoreProviderConfig() {
        return storeProviderConfig;
    }

    public void setStoreProviderConfig(StoreProviderConfig storeProviderConfig) {
        this.storeProviderConfig = storeProviderConfig;
    }

    public MailConfig getMailConfig() {
        return mailConfig;
    }

    public void setMailConfig(MailConfig mailConfig) {
        this.mailConfig = mailConfig;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SchedulerConfig)) return false;
        SchedulerConfig that = (SchedulerConfig) o;
        return Objects.equals(storeProviderConfig, that.storeProviderConfig) &&
                Objects.equals(mailConfig, that.mailConfig);
    }

    @Override
    public int hashCode() {

        return Objects.hash(storeProviderConfig, mailConfig);
    }

    @Override
    public String toString() {
        return "SchedulerConfig{" +
                "storeProviderConfig=" + storeProviderConfig +
                ", mailConfig=" + mailConfig +
                '}';
    }
}
