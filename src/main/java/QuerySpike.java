import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlMetrics;
import com.couchbase.client.java.query.N1qlQuery;

import java.util.List;


/**
 * Created by msaidi on 11/27/15.
 */
public class QuerySpike {

    // adjust to fit terminal
    static final int SCREENCOLUMNS = 132;

    public static void printDecoration(int c, String s) {
        for (int i = 0; i < c; i++) {
            System.out.print(s);
        }
    }

    public static void printCenteredBanner(String s) {
        int numDecorations = ((SCREENCOLUMNS - (s.length() + 2)) / 2);
        printDecoration(numDecorations, "=");
        System.out.print(" " + s + " ");
        printDecoration(numDecorations, "=");
        System.out.println();
    }

    // obtenez une liste des espaces de noms à partir des métadonnées du système
    public static List<N1qlQueryRow> getListOfNamespaces(Bucket b) {
        return queryOnSystem(b, "namespaces");
    }

    // Obtenir une liste de banques de données à partir des métadonnées du système
    public static List<N1qlQueryRow> getListOfDatastores(Bucket b) {
        return queryOnSystem(b, "datastores");
    }

    // Ceci est pour la recherche de certains types de métadonnées du système
    // Telles que le système: les espaces de noms ou système: des banques de données
    public static List<N1qlQueryRow> queryOnSystem(Bucket b, String subsystem) {
        return queryToListOfQueryRow(b, "SELECT * FROM system:" + subsystem);
    }

    // Cette méthode suppose que les documents aériennes contiennent un certain
    // Attribut, "type", et que la valeur de cet attribut est "airline".
    public static List<N1qlQueryRow> getListOfAirlines(Bucket b) {
        return getListByType(b, "airline");
    }

    // Assuming the document contain an attribute called type, this lets
    // you query the bucket for all documents of a certain type.
    public static List<N1qlQueryRow> getListByType(Bucket b, String typeValue) {
        return queryToListOfQueryRow(b, "SELECT * FROM `" + b.name() + "` where type=\"" + typeValue + "\";");
    }

    // convenience method.  Given a bucket and a query string, which is assumed to be
    // a SELECT, execute the query, print timing information, and return the results
    // to the caller, not as a QueryResult, but as a list of QueryRows which the
    // caller can iterate over and do things with.
    public static List<N1qlQueryRow> queryToListOfQueryRow(Bucket b, String queryString) {
        N1qlQueryResult queryResult = b.query(N1qlQuery.simple(queryString));
        List<N1qlQueryRow> rval = queryResult.allRows();
        N1qlMetrics qm = queryResult.info();
        System.out.println("The elapsed time of that query was:   " + qm.elapsedTime());
        System.out.println("The execution time of that query was: " + qm.executionTime());
        return rval;
    }

}
