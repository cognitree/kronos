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

package com.cognitree.kronos.scheduler.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

import static com.cognitree.kronos.scheduler.model.Schedule.Type.cron;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CronSchedule extends Schedule {
    private Type type = cron;
    private String cronExpression;
    private String timezone;

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CronSchedule)) return false;
        if (!super.equals(o)) return false;
        CronSchedule that = (CronSchedule) o;
        return type == that.type &&
                Objects.equals(cronExpression, that.cronExpression) &&
                Objects.equals(timezone, that.timezone);
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), type, cronExpression, timezone);
    }

    @Override
    public String toString() {
        return "CronSchedule{" +
                "type=" + type +
                ", cronExpression='" + cronExpression + '\'' +
                ", timezone='" + timezone + '\'' +
                "} " + super.toString();
    }
}