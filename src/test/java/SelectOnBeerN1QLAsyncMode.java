import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.AsyncN1qlQueryResult;
import com.couchbase.client.java.query.Index;
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
public class SelectOnBeerN1QLAsyncMode {

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
    public void selectOnBeerN1QLAsyncMode() {
        //select dans sample beer async bucket
        rx.Observable<AsyncN1qlQueryResult> resultObservable = beerSample.async().query(simple(select("*").from(i("beer-sample")).limit(10)));

        //manip async result
        resultObservable.subscribe(result -> {
            result.errors().subscribe(errors -> out.println(errors.toString()));
            result.info().subscribe(info -> {
                assertEquals(10, info.resultCount());
                out.println(info.elapsedTime());
                out.println(info.resultCount());
            });
        });
    }

    @After
    public void after() {
        cluster.disconnect();
    }
}
