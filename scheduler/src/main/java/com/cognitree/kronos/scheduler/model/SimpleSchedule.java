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

import static com.cognitree.kronos.scheduler.model.Schedule.Type.simple;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SimpleSchedule extends Schedule {
    private boolean repeatForever;
    private long repeatIntervalInMs = 0;
    private int repeatCount = 0;

    public SimpleSchedule() {
        super(simple);
    }

    public boolean isRepeatForever() {
        return repeatForever;
    }

    public void setRepeatForever(boolean repeatForever) {
        this.repeatForever = repeatForever;
    }

    public long getRepeatIntervalInMs() {
        return repeatIntervalInMs;
    }

    public void setRepeatIntervalInMs(long repeatIntervalInMs) {
        this.repeatIntervalInMs = repeatIntervalInMs;
    }

    public int getRepeatCount() {
        return repeatCount;
    }

    public void setRepeatCount(int repeatCount) {
        this.repeatCount = repeatCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleSchedule)) return false;
        SimpleSchedule that = (SimpleSchedule) o;
        return repeatForever == that.repeatForever &&
                repeatIntervalInMs == that.repeatIntervalInMs &&
                repeatCount == that.repeatCount;
    }

    @Override
    public int hashCode() {

        return Objects.hash(repeatForever, repeatIntervalInMs, repeatCount);
    }

    @Override
    public String toString() {
        return "SimpleSchedule{" +
                "repeatForever=" + repeatForever +
                ", repeatIntervalInMs=" + repeatIntervalInMs +
                ", repeatCount=" + repeatCount +
                "} " + super.toString();
    }
}