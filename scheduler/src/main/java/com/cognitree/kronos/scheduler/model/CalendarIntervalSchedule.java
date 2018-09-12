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
import org.quartz.DateBuilder.IntervalUnit;

import java.util.Objects;

import static com.cognitree.kronos.scheduler.model.Schedule.Type.calendar;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CalendarIntervalSchedule extends Schedule {

    private Type type = calendar;
    private int repeatInterval = 1;
    private IntervalUnit repeatIntervalUnit = IntervalUnit.DAY;
    private String timezone;
    private boolean preserveHourOfDayAcrossDaylightSavings;
    private boolean skipDayIfHourDoesNotExist;

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

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public boolean isPreserveHourOfDayAcrossDaylightSavings() {
        return preserveHourOfDayAcrossDaylightSavings;
    }

    public void setPreserveHourOfDayAcrossDaylightSavings(boolean preserveHourOfDayAcrossDaylightSavings) {
        this.preserveHourOfDayAcrossDaylightSavings = preserveHourOfDayAcrossDaylightSavings;
    }

    public boolean isSkipDayIfHourDoesNotExist() {
        return skipDayIfHourDoesNotExist;
    }

    public void setSkipDayIfHourDoesNotExist(boolean skipDayIfHourDoesNotExist) {
        this.skipDayIfHourDoesNotExist = skipDayIfHourDoesNotExist;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CalendarIntervalSchedule)) return false;
        if (!super.equals(o)) return false;
        CalendarIntervalSchedule that = (CalendarIntervalSchedule) o;
        return repeatInterval == that.repeatInterval &&
                preserveHourOfDayAcrossDaylightSavings == that.preserveHourOfDayAcrossDaylightSavings &&
                skipDayIfHourDoesNotExist == that.skipDayIfHourDoesNotExist &&
                type == that.type &&
                repeatIntervalUnit == that.repeatIntervalUnit &&
                Objects.equals(timezone, that.timezone);
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), type, repeatInterval, repeatIntervalUnit, timezone, preserveHourOfDayAcrossDaylightSavings, skipDayIfHourDoesNotExist);
    }

    @Override
    public String toString() {
        return "CalendarIntervalSchedule{" +
                "type=" + type +
                ", repeatIntervalUnit=" + repeatIntervalUnit +
                ", repeatInterval=" + repeatInterval +
                ", timezone='" + timezone + '\'' +
                ", preserveHourOfDayAcrossDaylightSavings=" + preserveHourOfDayAcrossDaylightSavings +
                ", skipDayIfHourDoesNotExist=" + skipDayIfHourDoesNotExist +
                "} " + super.toString();
    }
}