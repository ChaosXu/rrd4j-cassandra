package com.chaosxu.rrd4j.backend.cassandra;

import org.junit.Before;
import org.junit.Test;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;
import org.rrd4j.core.Util;

import java.io.IOException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import static org.rrd4j.ConsolFun.AVERAGE;
import static org.rrd4j.DsType.GAUGE;

/**
 * @author chaos
 */
public class PerformanceTest {
    private static final int rrdCount = 10000;

    private List<RrdDb> rrdDbs = new LinkedList<RrdDb>();

    @Before
    public void init() throws IOException {
        CassandraBackendFactory factory = new CassandraBackendFactory("localhost:9160");

        TimeTrace timeTrace = new TimeTrace();
        timeTrace.begin();
        for (int i = 0; i < rrdCount; i++) {
            RrdDef rrdDef = new RrdDef("pt-5m-" + i, 0, 300);
            rrdDef.setVersion(2);
            rrdDef.addDatasource("ds", GAUGE, 3600, -5, 30);
            rrdDef.addArchive(AVERAGE, 0.5, 300, 1000);
            rrdDbs.add(new RrdDb(rrdDef, factory));
        }
        timeTrace.end();
        System.out.printf("init time(Total):%s,count:%s\r\n", timeTrace.getMills(), rrdDbs.size());
        System.out.printf("init time(Avg):%s\r\n", timeTrace.getMills() / rrdDbs.size());
    }

    @Test
    public void testSetValue() throws IOException {
        Calendar testTime = Calendar.getInstance();
        testTime.set(Calendar.MINUTE, 0);
        testTime.set(Calendar.SECOND, 0);
        testTime.set(Calendar.MILLISECOND, 0);

        long start = Util.getTimestamp(testTime);
        TimeTrace timeTrace = new TimeTrace();
        timeTrace.begin();
        for (RrdDb rrdDb : rrdDbs) {
            long timeStamp = start;
            for (int i = 0; i < 1; i++) {
                long sampleTime = timeStamp;
                rrdDb.createSample(sampleTime).setValue("ds", 30).update();
                timeStamp += 300;
            }
        }
        timeTrace.end();
        System.out.printf("setValue time(Total):%s,count:%s\r\n", timeTrace.getMills(), rrdDbs.size());
        System.out.printf("setValue time(Avg):%s\r\n", timeTrace.getMills() / rrdDbs.size());
        closeDb();
    }

    private void closeDb() throws IOException {
        TimeTrace timeTrace = new TimeTrace();
        timeTrace.begin();
        for (RrdDb rrdDb : rrdDbs) {
            rrdDb.close();
        }
        timeTrace.end();
        System.out.printf("closeDb time(Total):%s,count:%s\r\n", timeTrace.getMills(), rrdDbs.size());
        System.out.printf("closeDb time(Avg):%s\r\n", timeTrace.getMills() / rrdDbs.size());
    }
}
