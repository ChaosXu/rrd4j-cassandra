package com.chaosxu.rrd4j.backend.cassandra;

import org.junit.Before;
import org.junit.Test;
import org.rrd4j.core.FetchData;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;
import org.rrd4j.core.Util;

import java.io.IOException;
import java.util.Calendar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.rrd4j.ConsolFun.AVERAGE;
import static org.rrd4j.DsType.GAUGE;

/**
 * @author chaos
 */
public class CassandraBackendTest {
    private CassandraBackendFactory factory;

    @Before
    public void init(){
        factory = new CassandraBackendFactory("localhost:9160");
    }

    @Test
    public void testSpike() throws IOException {
        RrdDef rrdDef = new RrdDef("testSpike.rrd", 0, 60);
        rrdDef.setVersion(2);
        rrdDef.addDatasource("ds", GAUGE, 3600, -5, 30);
        rrdDef.addArchive(AVERAGE, 0.5, 60, 999);
        RrdDb rrdDb = new RrdDb(rrdDef,factory);

        Calendar testTime = Calendar.getInstance();
        testTime.set(Calendar.MINUTE, 0);
        testTime.set(Calendar.SECOND, 0);
        testTime.set(Calendar.MILLISECOND, 0);
        System.out.println(testTime);
        //testTime.add(Calendar.HOUR, -1);
        long start =  Util.getTimestamp(testTime);
        long timeStamp = start;

        for(int i = 0; i < 180; i++) {
            long  sampleTime = timeStamp;
            if(i == 117) {
                sampleTime += -1;
            }
            rrdDb.createSample(sampleTime).setValue("ds", 30).update();
            timeStamp += 60;
        }

        long end = timeStamp;
        FetchData f = rrdDb.createFetchRequest(AVERAGE, start, end).fetchData();
        System.out.println(f.dump());
        double[] values = f.getValues("ds");

        assertTrue("Data before first entry", Double.isNaN(values[0]));
        assertEquals("Bad average in point 1", 30, values[1], 1e-3);
        assertEquals("Bad average in point 2", 30, values[2], 1e-3);
        assertTrue("Data after last entry", Double.isNaN(values[3]));

        rrdDb.close();

        RrdDb rrdDbForRead = new RrdDb("testSpike.rrd",factory);
        FetchData result = rrdDbForRead.createFetchRequest(AVERAGE, start, end).fetchData();
        System.out.println(f.dump());
        double[] resultData = result.getValues("ds");

        assertTrue("Data before first entry", Double.isNaN(resultData[0]));
        assertEquals("Bad average in point 1", 30, resultData[1], 1e-3);
        assertEquals("Bad average in point 2", 30, resultData[2], 1e-3);
        assertTrue("Data after last entry", Double.isNaN(resultData[3]));

        rrdDbForRead.close();
    }
}
