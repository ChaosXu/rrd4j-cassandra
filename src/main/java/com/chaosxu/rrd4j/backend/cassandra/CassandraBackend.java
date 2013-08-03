package com.chaosxu.rrd4j.backend.cassandra;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import org.rrd4j.core.RrdByteArrayBackend;

import java.io.IOException;

/**
 * Backend which is used to store RRD data in cassandra.
 *
 * @author chaos
 */
class CassandraBackend extends RrdByteArrayBackend {

    public static final String RRD = "rrd";
    private final ColumnFamilyTemplate<String,String> template;
    private boolean dirty;
    private String columnName;

    /**
     * <p>Constructor for RrdMongoDBBackend.</p>
     *
     * @param path a {@link java.lang.String} object.
     * @param ksp  a {@link Keyspace} object.
     * @param columnFamily name of cassandra column family
     */
    protected CassandraBackend(String path, Keyspace ksp, String columnFamily) {
        super(path);

        template = new ThriftColumnFamilyTemplate<String, String>(ksp,
                columnFamily,
                StringSerializer.get(),
                StringSerializer.get());
        ColumnFamilyResult<String,String> columnFamilyResult = template.queryColumns(getPath());
        if(columnFamilyResult.hasResults()){
            buffer = columnFamilyResult.getByteArray(RRD);
        }
    }

    @Override
    protected synchronized void write(long offset, byte[] bytes) throws IOException {
        super.write(offset, bytes);
        dirty = true;
    }


    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        if (dirty) {
            ColumnFamilyUpdater<String, String> updater = template.createUpdater(getPath());
            updater.setByteArray(RRD, buffer);
            template.update(updater);
        }
    }
}
