import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.Index;
import com.couchbase.client.java.query.N1qlQueryResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.couchbase.client.java.query.N1qlQuery.simple;
import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;
import static java.lang.System.out;
import static org.junit.Assert.assertEquals;

/**
 * Created by msaidi on 11/26/15.
 */
public class SelectOnBeerN1QLSyncMode {
    private Bucket beerSample;
    private CouchbaseCluster cluster;

    @Before
    public void before() {
        CouchbaseEnvironment environment = DefaultCouchbaseEnvironment.create();
        cluster = CouchbaseCluster.create(environment);
        beerSample = cluster.openBucket("beer-sample");

        //use N1QL
        beerSample.query(simple(Index.createPrimaryIndex().on("beer-sample")));
    }

    @Test
    public void selectOnBeerTest() {
        // select query
        N1qlQueryResult n1qlQueryRows = beerSample.query(simple(select("*").from(i("beer-sample")).limit(10)));

        assertEquals(10, n1qlQueryRows.info().resultCount());

        out.println(n1qlQueryRows.errors().toString());
        out.println(n1qlQueryRows.info().elapsedTime());
        out.println(n1qlQueryRows.info().resultCount());
    }

    @After
    public void after() {
        cluster.disconnect();
    }

}
