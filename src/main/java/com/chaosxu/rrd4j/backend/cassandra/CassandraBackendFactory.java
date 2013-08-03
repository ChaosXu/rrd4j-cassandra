package com.chaosxu.rrd4j.backend.cassandra;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import org.rrd4j.core.RrdBackend;
import org.rrd4j.core.RrdBackendFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * Create CassandraBackend for data storage.
 *
 * @author chaos
 */
public class CassandraBackendFactory extends RrdBackendFactory {

    private static final String KEYSPACE = "ks_rrd4j";
    private static final String COLUMN_FAMILY_NAME = "cf_rrd4j";
    private final Keyspace ksp;

    public CassandraBackendFactory(String hostIp) {
        Cluster cluster = HFactory.getOrCreateCluster("rrd4j-cluster", hostIp);

        KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(KEYSPACE);

        if (keyspaceDef == null)
            createSchema(cluster, KEYSPACE);

        ksp = HFactory.createKeyspace(KEYSPACE, cluster);
    }

    private void createSchema(Cluster cluster, String keyspace) {
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace,
                COLUMN_FAMILY_NAME,
                ComparatorType.BYTESTYPE);

        KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspace,
                ThriftKsDef.DEF_STRATEGY_CLASS,
                1,
                Arrays.asList(cfDef));

        cluster.addKeyspace(newKeyspace, true);
    }

    /**
     * Creates RrdBackend object for the given storage path.
     *
     * @param path     Storage path
     * @param readOnly True, if the storage should be accessed in read/only mode.
     *                 False otherwise.
     * @return Backend object which handles all I/O operations for the given storage path
     * @throws IOException Thrown in case of I/O error.
     */
    @Override
    protected RrdBackend open(String path, boolean readOnly) throws IOException {
        return new CassandraBackend(path, ksp, COLUMN_FAMILY_NAME);
    }

    /**
     * Determines if a storage with the given path already exists.
     *
     * @param path Storage path
     * @return True, if such storage exists, false otherwise.
     */
    @Override
    protected boolean exists(String path) throws IOException {
        ColumnFamilyTemplate<String, String> template =
                new ThriftColumnFamilyTemplate<String, String>(ksp,
                        COLUMN_FAMILY_NAME,
                        StringSerializer.get(),
                        StringSerializer.get());
        ColumnFamilyResult<String, String> res = template.queryColumns(path);
        return res.hasResults();
    }

    /**
     * Determines if the header should be validated.
     *
     * @param path Storage path
     * @return True, if the header should be validated for this factory
     * @throws IOException if header validation fails
     */
    @Override
    protected boolean shouldValidateHeader(String path) throws IOException {
        return false;
    }

    /**
     * Returns the name (primary ID) for the factory.
     *
     * @return Name of the factory.
     */
    @Override
    public String getName() {
        return "cassandra";
    }
}
