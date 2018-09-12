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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.quartz.DailyTimeIntervalTrigger;
import org.quartz.DateBuilder.IntervalUnit;

import java.util.Objects;
import java.util.Set;

import static com.cognitree.kronos.scheduler.model.DailyTimeIntervalSchedule.TimeOfDay.END_OF_DAY;
import static com.cognitree.kronos.scheduler.model.DailyTimeIntervalSchedule.TimeOfDay.START_OF_DAY;
import static com.cognitree.kronos.scheduler.model.Schedule.Type.daily_time;
import static org.quartz.DailyTimeIntervalScheduleBuilder.ALL_DAYS_OF_THE_WEEK;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DailyTimeIntervalSchedule extends Schedule {
    private Type type = daily_time;
    private int repeatInterval = 1;
    private IntervalUnit repeatIntervalUnit = IntervalUnit.MINUTE;
    private int repeatCount = DailyTimeIntervalTrigger.REPEAT_INDEFINITELY;
    private TimeOfDay startTimeOfDay = START_OF_DAY;
    private TimeOfDay endTimeOfDay = END_OF_DAY;
    private Set<Integer> daysOfWeek = ALL_DAYS_OF_THE_WEEK;

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }

    public int getRepeatInterval() {
        return repeatInterval;
    }

    public void setRepeatInterval(int repeatInterval) {
        this.repeatInterval = repeatInterval;
    }

    public IntervalUnit getRepeatIntervalUnit() {
        return repeatIntervalUnit;
    }

    public void setRepeatIntervalUnit(IntervalUnit repeatIntervalUnit) {
        this.repeatIntervalUnit = repeatIntervalUnit;
    }

    public int getRepeatCount() {
        return repeatCount;
    }

    public void setRepeatCount(int repeatCount) {
        this.repeatCount = repeatCount;
    }

    public TimeOfDay getStartTimeOfDay() {
        return startTimeOfDay;
    }

    public void setStartTimeOfDay(TimeOfDay startTimeOfDay) {
        this.startTimeOfDay = startTimeOfDay;
    }

    public TimeOfDay getEndTimeOfDay() {
        return endTimeOfDay;
    }

    public void setEndTimeOfDay(TimeOfDay endTimeOfDay) {
        this.endTimeOfDay = endTimeOfDay;
    }

    public Set<Integer> getDaysOfWeek() {
        return daysOfWeek;
    }

    public void setDaysOfWeek(Set<Integer> daysOfWeek) {
        this.daysOfWeek = daysOfWeek;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DailyTimeIntervalSchedule)) return false;
        if (!super.equals(o)) return false;
        DailyTimeIntervalSchedule that = (DailyTimeIntervalSchedule) o;
        return repeatInterval == that.repeatInterval &&
                repeatCount == that.repeatCount &&
                type == that.type &&
                repeatIntervalUnit == that.repeatIntervalUnit &&
                Objects.equals(startTimeOfDay, that.startTimeOfDay) &&
                Objects.equals(endTimeOfDay, that.endTimeOfDay) &&
                Objects.equals(daysOfWeek, that.daysOfWeek);
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), type, repeatInterval, repeatIntervalUnit, repeatCount, startTimeOfDay, endTimeOfDay, daysOfWeek);
    }

    @Override
    public String toString() {
        return "DailyTimeIntervalSchedule{" +
                "type=" + type +
                ", repeatIntervalUnit=" + repeatIntervalUnit +
                ", repeatCount=" + repeatCount +
                ", repeatInterval=" + repeatInterval +
                ", startTimeOfDay=" + startTimeOfDay +
                ", endTimeOfDay=" + endTimeOfDay +
                ", daysOfWeek=" + daysOfWeek +
                "} " + super.toString();
    }

    public static class TimeOfDay {

        public static final TimeOfDay START_OF_DAY = new TimeOfDay(0, 0, 0);
        public static final TimeOfDay END_OF_DAY = new TimeOfDay(23, 59, 59);

        private int hour;
        private int minute;
        private int second;

        @JsonCreator
        public TimeOfDay(@JsonProperty("hour") int hour,
                         @JsonProperty("minute") int minute,
                         @JsonProperty("second") int second) {
            this.hour = hour;
            this.minute = minute;
            this.second = second;
        }

        public int getHour() {
            return hour;
        }

        public void setHour(int hour) {
            this.hour = hour;
        }

        public int getMinute() {
            return minute;
        }

        public void setMinute(int minute) {
            this.minute = minute;
        }

        public int getSecond() {
            return second;
        }

        public void setSecond(int second) {
            this.second = second;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TimeOfDay)) return false;
            TimeOfDay timeOfDay = (TimeOfDay) o;
            return hour == timeOfDay.hour &&
                    minute == timeOfDay.minute &&
                    second == timeOfDay.second;
        }

        @Override
        public int hashCode() {

            return Objects.hash(hour, minute, second);
        }

        @Override
        public String toString() {
            return "TimeOfDay{" +
                    "hour=" + hour +
                    ", minute=" + minute +
                    ", second=" + second +
                    '}';
        }
    }
}