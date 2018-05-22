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

package com.cognitree.tasks.model;

import java.util.Objects;

import static com.cognitree.tasks.model.TaskDependencyInfo.Mode.all;

/**
 * defines dependency between two tasks
 */
public class TaskDependencyInfo {
    /**
     * name of the task it depends on
     */
    private String name;
    /**
     * <p>
     * time period to look for all the tasks with name {@link TaskDependencyInfo#name} to associate dependency.
     * <p>
     * duration lets a user clearly define which instance of task it depends on by defining a boundary
     */
    private String duration;
    /**
     * mode of dependency whether it depends on the first last or all tasks found in the
     * {@link TaskDependencyInfo#duration} period
     */
    private Mode mode = all;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskDependencyInfo)) return false;
        TaskDependencyInfo that = (TaskDependencyInfo) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(duration, that.duration) &&
                mode == that.mode;
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, duration, mode);
    }

    @Override
    public String toString() {
        return "TaskDependencyInfo{" +
                "name='" + name + '\'' +
                ", duration='" + duration + '\'' +
                ", mode=" + mode +
                '}';
    }

    public enum Mode {
        first, last, all
    }
}