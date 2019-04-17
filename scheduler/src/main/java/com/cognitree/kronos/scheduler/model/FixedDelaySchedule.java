package com.cognitree.kronos.scheduler.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

import static com.cognitree.kronos.scheduler.model.Schedule.Type.fixed;

/**
 * Allows to schedule workflow at a fixed delay.
 * <p>
 * In fixed-delay execution, each execution is scheduled relative to
 * the actual execution time of the previous execution.  If an execution
 * is delayed for any reason (such as garbage collection or other
 * background activity), subsequent executions will be delayed as well
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FixedDelaySchedule extends Schedule {
    private Type type = fixed;
    private int interval;

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        FixedDelaySchedule that = (FixedDelaySchedule) o;
        return interval == that.interval &&
                type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), type, interval);
    }

    @Override
    public String toString() {
        return "FixedDelaySchedule{" +
                "type=" + type +
                ", interval=" + interval +
                '}';
    }
}
