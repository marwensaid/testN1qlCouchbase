import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlMetrics;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;


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

    /**
     * Méthode pratique. Compte tenu d'un seau et une String de requête, qui est supposé être
     * SELECT, exécuter la requête, l'impression des informations de synchronisation, et retourner les résultats
     * À l'appelant, non pas comme un QueryResult, mais comme une liste de QueryRows laquelle le
     * Appelant peut parcourir et faire des choses avec.*
     */
    public static List<N1qlQueryRow> queryToListOfQueryRow(Bucket bucket, String queryString) {
        N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(queryString));
        List<N1qlQueryRow> resultAllRows = queryResult.allRows();
        N1qlMetrics queryMetric = queryResult.info();
        System.out.println("The elapsed time of that query was:   " + queryMetric.elapsedTime());
        System.out.println("The execution time of that query was: " + queryMetric.executionTime());
        return resultAllRows;
    }

    // donner un String et un Integer dans une hashtable et voir la output
    public static void printHashtable(Hashtable<String, Integer> siHashtable) {

        System.out.println("Key                        Value     ");
        System.out.println("-------------------------  ----------");

        Set<String> keyList = siHashtable.keySet();

        for (String eachKey : keyList) {
            System.out.printf("%25s  %10d\n", eachKey, siHashtable.get(eachKey));
        }

    }

    public static Hashtable<String,Integer> queryRowListToHashtable(List<N1qlQueryRow> listOfQueryRow, String keyColName, String valColName) {
        // Given query results and the name of the key column and the name of the
        // value column, create and return a Hashtable containing the keys and
        // values
        Hashtable<String,Integer> result = new Hashtable<String, Integer>();

        String eachKey = null;
        int eachValue  = 0;

        JsonObject eachJsonObject = null;

        for (N1qlQueryRow query : listOfQueryRow) {
            eachJsonObject = query.value();
            eachKey = eachJsonObject.getString(keyColName);
            eachValue = eachJsonObject.getInt(valColName);
            result.put(eachKey, eachValue);
        }

        return result;
    }

    // Given a list of query rows which are known to contain index info, use Gson to
    // create the POJOs and put them into a list and return the list.
    public static List<IndexInfo> getIndexInfoListFromQueryRowList(List<N1qlQueryRow> listOfQueryRow) {

        List<IndexInfo> resultValue = new ArrayList<IndexInfo>();
        Gson gson = new Gson();
        IndexInfo eachIndexInfo = null;
        String iiString;
        JsonObject eachIndexInfoJsonObject;

        for (N1qlQueryRow query : listOfQueryRow) {
            eachIndexInfoJsonObject = query.value().getObject("indexes");
            iiString = eachIndexInfoJsonObject.toString();
            // System.out.println("each index info string is:" + iiString);
            eachIndexInfo = gson.fromJson(iiString, IndexInfo.class);
            resultValue.add(eachIndexInfo);
        }

        return resultValue;

    }

    // Given a list of IndexInfo POJOs, print them out
    public static void printIndexInfoList(List<IndexInfo> indexInfoList) {

        System.out.println("Name                  ID                   State           Keyspace       ");
        System.out.println("--------------------  -------------------- --------------- ---------------");

        for (IndexInfo ii : indexInfoList) {
            System.out.printf("%20s %20s %15s %15s\n", ii.name, ii.id, ii.state, ii.keyspace_id);
        }

    }

    // Given a bucket, get a list of objects containing the information about the
    // indexes in the bucket
    public static List<IndexInfo> getIndexInfoList(Bucket b) {
        return getIndexInfoListFromQueryRowList(queryOnSystem(b, "indexes"));
    }

    // Given a list of query rows, print the raw json for each
    public static void printRawJson(List<N1qlQueryRow> listOfQueryRow) {
        JsonObject eachJsonObject = null;
        int i = 0;

        for (N1qlQueryRow qr : listOfQueryRow) {
            eachJsonObject = qr.value();
            System.out.printf("%2d : %10s\n", i, eachJsonObject);
            i++;
        }

    }

    // Assumes that each of the items in the list is a QueryRow
    // whose value is a JsonObject, and that that JsonObject contains
    // an attribute called "namespaces", which then contains another JsonObject
    // that has an attribute called "name"
    public static void printListOfNamespaces(List<N1qlQueryRow> namespaceList) {
        JsonObject eachJsonObject = null;
        JsonObject eachNamespaceObject = null;
        int i = 0;
        String eachNamespaceName = null;

        for (N1qlQueryRow qr : namespaceList) {
            eachJsonObject = qr.value();
            eachNamespaceObject = eachJsonObject.getObject("namespaces");
            eachNamespaceName = eachNamespaceObject.getString("name");
            System.out.printf("%2d : %10s\n", i, eachNamespaceName);
            i++;
        }

    }

    // Same as above but makes different assumptions about the JsonObject
    // contained in each QueryRow in the list
    public static void printListOfDatastores(List<N1qlQueryRow> datastoreList) {
        JsonObject eachJsonObject = null;
        JsonObject eachDatastoreObject = null;
        int i = 0;
        String eachDatastoreId = null;

        for (N1qlQueryRow qr : datastoreList) {
            eachJsonObject = qr.value();
            eachDatastoreObject = eachJsonObject.getObject("datastores");
            eachDatastoreId = eachDatastoreObject.getString("id");
            System.out.printf("%2d : %10s\n", i, eachDatastoreId);
            i++;
        }

    }


}
