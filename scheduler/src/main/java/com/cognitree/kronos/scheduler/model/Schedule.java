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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.quartz.Trigger;

import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SimpleSchedule.class, name = "simple"),
        @JsonSubTypes.Type(value = CronSchedule.class, name = "cron"),
        @JsonSubTypes.Type(value = DailyTimeIntervalSchedule.class, name = "daily_time"),
        @JsonSubTypes.Type(value = CalendarIntervalSchedule.class, name = "calendar")
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class Schedule {
    private Type type;
    private int misfireInstruction = Trigger.MISFIRE_INSTRUCTION_SMART_POLICY;

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public int getMisfireInstruction() {
        return misfireInstruction;
    }

    public void setMisfireInstruction(int misfireInstruction) {
        this.misfireInstruction = misfireInstruction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Schedule)) return false;
        Schedule schedule = (Schedule) o;
        return misfireInstruction == schedule.misfireInstruction &&
                type == schedule.type;
    }

    @Override
    public int hashCode() {

        return Objects.hash(type, misfireInstruction);
    }

    @Override
    public String toString() {
        return "Schedule{" +
                "type=" + type +
                ", misfireInstruction=" + misfireInstruction +
                '}';
    }

    public enum Type {
        cron, simple, daily_time, calendar
    }
}
