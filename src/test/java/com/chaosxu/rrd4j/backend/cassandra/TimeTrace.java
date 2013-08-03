package com.chaosxu.rrd4j.backend.cassandra;

/**
 * @author chaos
 */
public class TimeTrace {
    private long beginTime;
    private long endTime;

    public void begin() {
        this.beginTime = System.currentTimeMillis();
    }

    public void end() {
        this.endTime = System.currentTimeMillis();
    }

    public long getMills() {
        return endTime - beginTime;
    }
}
