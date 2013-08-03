package hector;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

/**
 * @author chaos
 */
public class HectorTest {

    public static final String KEYSPACE = "TestKeyspace";
    public static final String COLUMN_FAMILY_NAME = "ColumnFamilyName";

    @Test
    public void test() {
        Cluster cluster = HFactory.getOrCreateCluster("test-cluster","localhost:9160");

        KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(KEYSPACE);

        if (keyspaceDef == null)
            createSchema(cluster, KEYSPACE);

        Keyspace ksp = HFactory.createKeyspace(KEYSPACE, cluster);

        ColumnFamilyTemplate<String, String> template =
                new ThriftColumnFamilyTemplate<String, String>(ksp,
                        COLUMN_FAMILY_NAME,
                        StringSerializer.get(),
                        StringSerializer.get());

        update(template);

        query(ksp);

        //delete(template);

    }

    private void query(Keyspace ksp) {
        SliceQuery<String, String, String> query = HFactory.createSliceQuery(ksp, StringSerializer.get(),
                StringSerializer.get(), StringSerializer.get()).
                setKey("a key").setColumnFamily(COLUMN_FAMILY_NAME);

        ColumnSliceIterator<String, String, String> iterator =
                new ColumnSliceIterator<String, String, String>(query, null, "\uFFFF", false);

        while (iterator.hasNext()) {
            HColumn column = iterator.next();
            System.out.printf("%s:%s \r\n",column.getName(),column.getValue());
        }
    }

    private void delete(ColumnFamilyTemplate<String, String> template) {
        ColumnFamilyResult<String, String> res = template.queryColumns("a key");
        String value = res.getString("domain");
        template.deleteColumn("key", "column name");
    }

    private void update(ColumnFamilyTemplate<String, String> template) {
        ColumnFamilyUpdater<String, String> updater = template.createUpdater("a key");
        updater.setString("domain", "www.chaosxu.com");
        updater.setLong("time", System.currentTimeMillis());

        template.update(updater);
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
}
