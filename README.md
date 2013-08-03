rrd4j-cassandra
===============

A backend of rrd4j for store data in cassandra

Sample
-----
```java
import com.chaosxu.rrd4j.backend.cassandra.CassandraBackendFactory
...

CassandraBackendFactory factory = new CassandraBackendFactory("localhost:9160");
RrdDb rrdDb = new RrdDb("server",factory);
```