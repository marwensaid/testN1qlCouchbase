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
    public static List<N1qlQueryRow> getListOfNamespaces(Bucket bucket) {
        return queryOnSystem(bucket, "namespaces");
    }

    // Obtenir une liste de banques de données à partir des métadonnées du système
    public static List<N1qlQueryRow> getListOfDatastores(Bucket bucket) {
        return queryOnSystem(bucket, "datastores");
    }

    // Ceci est pour la recherche de certains types de métadonnées du système telles que le système:
    // les espaces de noms ou système: des banques de données
    public static List<N1qlQueryRow> queryOnSystem(Bucket bucket, String subsystem) {
        return queryToListOfQueryRow(bucket, "SELECT * FROM system:" + subsystem);
    }

    // Cette méthode suppose que les documents aériennes contiennent un certain attribut, "type", et que la valeur de cet attribut est "airline".
    public static List<N1qlQueryRow> getListOfAirlines(Bucket bucket) {
        return getListByType(bucket, "airline");
    }

    // En supposant que le document contient un attribut type dit, cela permet vous interrogez le seau pour tous les documents d'un certain type.
    public static List<N1qlQueryRow> getListByType(Bucket bucket, String typeValue) {
        return queryToListOfQueryRow(bucket, "SELECT * FROM `" + bucket.name() + "` where type=\"" + typeValue + "\";");
    }

    /** Méthode pratique. Compte tenu d'un seau et une chaîne de requête, qui est supposé être
     SELECT, exécuter la requête, l'impression des informations de synchronisation, et retourner les résultats
     À l'appelant, non pas comme un QueryResult, mais comme une liste de QueryRows laquelle le
     Appelant peut parcourir et faire des choses avec.**/
    public static List<N1qlQueryRow> queryToListOfQueryRow(Bucket bucket, String queryString) {
        N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(queryString));
        List<N1qlQueryRow> rval = queryResult.allRows();
        N1qlMetrics qm = queryResult.info();
        System.out.println("The elapsed time of that query was:   " + qm.elapsedTime());
        System.out.println("The execution time of that query was: " + qm.executionTime());
        return rval;
    }

}
