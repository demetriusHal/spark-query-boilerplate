package rdfparsercheckuoa.rdf_parser_check;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;


import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.Collections;




public class App {
    static SparkSession spark;
    static String name_tripletable = "prost_test.triples";
    static String column_name_subject = "s";
    static String column_name_predicate = "p";
    static String column_name_object = "o";
    static String tableName = "triples";
    static String dbName = "prost_test";
    static String mixedTable = "tmp1";
    static String tableSpecifier = "prost_test.triples";
    static String distinctTable = "triples_distinct";

    public static void main(String[] args) {


        App.spark = SparkSession.builder().appName("JD Word Counter").enableHiveSupport().getOrCreate();
        //App.spark.sql(queryDropTripleTable);
        //App.spark.sql(queryDropTripleTableFixed);

        prefixEncode();
        spark.sql("select count(*)  from prost_test.double_encoded").show();
        spark.sql("select count(*)  from prost_test.triples").show();

    }

    private static void encode() {
        Dataset<Row> res = spark.sql("SELECT s FROM prost_test" + tableName);
    }

    public static List<String> splitURI(Row item) {
        String URI = item.getString(0);
        if (!URI.startsWith("http"))
            return new ArrayList<String>();
        List<String> arr = Arrays.asList(URI.split("/", 0));
        //TODO comment this!
        //Collections.reverse(arr);
        return arr;

    }

    public static String reconstructURI(List<String> ls, int num) {
        String start = "http:/";
        int i;
        for (i = 1; i < num && i < ls.size() - 1; i++) {
            start = start + ls.get(i) + "/";
        }
        return start + ls.get(i);
    }

    public static String reconURI(List<String> ls, int start, int end, String prefix) {
        int i;
        for (i = start; i < end && i < ls.size() - 1; i++) {
            prefix = prefix + ls.get(i) + "/";
        }
        if (i < ls.size())
            return prefix + ls.get(i);
        return prefix;
    }

    private static void runSql() {
        //drop tables first!
        spark.sql("DROP TABLE IF EXISTS prost_test.tmp1");
        spark.sql("DROP TABLE IF EXISTS prost_test.triples_distinct");

        String insertSubjects = String.format("CREATE TABLE %1$s.%2$s  AS (SELECT %3$s FROM %4$s)",
                dbName, mixedTable, column_name_subject, tableSpecifier);
        String insertObjects = String.format("INSERT INTO TABLE %1$s.%2$s SELECT %3$s FROM %4$s WHERE OType=2",
                dbName, mixedTable, column_name_object, tableSpecifier);
        String createDistinctTable = String.format("CREATE TABLE %1$s.%2$s  AS (SELECT DISTINCT(s) FROM %1$s.%3$s)",
                dbName, distinctTable, mixedTable);

        spark.sql(insertSubjects);
        spark.sql(insertObjects);
        Dataset<Row> res = spark.sql(createDistinctTable);
        res = spark.sql("SELECT * FROM prost_test.triples_distinct");
        res.show();
        JavaRDD<Row> data = res.toJavaRDD();
        JavaRDD<List<String>> liststrings = data.map(item -> splitURI(item));
        List<List<String>> list = liststrings.collect();

        int i;
        for (i = 0; i < 10; i++) {
            final int k = i;
//            JavaRDD<String> reconstructedURI = liststrings.map(newlist -> reconstructURI(newlist, k));
//            System.out.println("----->"+reconstructedURI.distinct().count());
//            JavaPairRDD<String, Integer> results = reconstructedURI.mapToPair(s -> new Tuple2(s, 1)).reduceByKey((x, y) -> (int) x + (int) y);
//            JavaPairRDD<String, Integer> sortedResults = results.mapToPair(p -> new Tuple2(p._2(), p._1())).sortByKey(false);
//            List<Tuple2<String, Integer>> slist = results.collect();
//            for (i = 0; i < slist.size() && i < 10; i++) {
//                System.out.println("-----" + slist.get(i));
//            }

            JavaRDD<String> column = liststrings.map(newlist -> {
                if (newlist.size() > k) return newlist.get(k);
                else
                    return null;
            });

            System.out.println("----->" + column.distinct().count());
            collectAndPrint(column.distinct());

        }


    }

    private static void collectAndPrint(JavaRDD<String> sl) {
        List<String> slist = sl.collect();
        for (int i = 0; i < slist.size() && i < 20; i++) {
            System.out.println("-----#" + slist.get(i));

        }
    }


    private static void wordCount(String fileName, String outfile) {

        SparkConf sparkConf = new SparkConf().setAppName("JD Word Counter");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> inputFile = sparkContext.textFile(fileName);
        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
        JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);
        countData.saveAsTextFile(outfile);
    }

    private static void createDistinct() {
        spark.sql("DROP TABLE IF EXISTS prost_test.triples_distinct");
        spark.sql("DROP TABLE IF EXISTS prost_test.td1");
        spark.sql("DROP TABLE IF EXISTS prost_test.td2");


        spark.sql("CREATE TABLE prost_test.td1  AS (SELECT s FROM prost_test.triples)");
        spark.sql("INSERT INTO prost_test.td1 (SELECT o FROM prost_test.triples WHERE OType=2)");
        spark.sql("CREATE TABLE prost_test.triples_distinct AS (SELECT DISTINCT(s) FROM prost_test.td1)");
    }

    private static void prefixEncode() {
        createDistinct();
        Dataset<Row> res;
        res = spark.sql("SELECT * FROM prost_test.triples_distinct");
        res.show();
        JavaRDD<Row> data = res.toJavaRDD();
//        JavaRDD<List<String>> liststrings = data.map(item -> splitURI(item));
//        JavaRDD<String> reconstructedURI = liststrings.map(newlist -> reconURI(newlist, 1, 6, "https:/"));
//        JavaRDD<String> restURI = liststrings.map(newlist -> reconURI(newlist, 7, -1, ""));

        JavaRDD<URITable> list = data.map(item -> new URITable(item));
        Dataset<Row> dataset = spark.createDataFrame(list, URITable.class);
        dataset.createOrReplaceTempView("tempTable");

        System.out.println(5);
        spark.sql("DROP TABLE IF EXISTS prost_test.triples_split");
        spark.sql("CREATE TABLE IF NOT EXISTS prost_test.triples_split AS SELECT * FROM tempTable");


        spark.sql("DROP TABLE IF EXISTS prost_test.right_dict");
        spark.sql("DROP TABLE IF EXISTS prost_test.left_dict");
        spark.sql("DROP TABLE IF EXISTS prost_test.tripl_left");
        spark.sql("DROP TABLE IF EXISTS prost_test.tripl_right");
        spark.sql("DROP TABLE IF EXISTS prost_test.mixed");
        spark.sql("DROP TABLE IF EXISTS prost_test.double_encoded");



        spark.sql("CREATE TABLE IF NOT EXISTS prost_test.tripl_left AS SELECT DISTINCT left from prost_test.triples_split");
        spark.sql("CREATE TABLE IF NOT EXISTS prost_test.tripl_right AS SELECT DISTINCT right from prost_test.triples_split");

        spark.sql("CREATE TABLE prost_test.left_dict  AS (SELECT left as key, concat('l', (row_number() over (order by left))) value FROM prost_test.tripl_left)");
        spark.sql("CREATE TABLE prost_test.right_dict  AS (SELECT right as key, concat('r', (row_number() over (order by right))) value FROM prost_test.tripl_right)");

        spark.sql("CREATE TABLE prost_test.mixed AS (SELECT t.full, t.left, l.value as lvalue, t.right, r.value as rvalue FROM prost_test.triples_split t, prost_test.left_dict l, prost_test.right_dict r WHERE t.left = l.key AND t.right = r.key)");
        spark.sql("DROP TABLE IF EXISTS prost_test.double_encoded");
        spark.sql("DROP TABLE IF EXISTS prost_test.double_dictionary");
        //spark.sql("CREATE TABLE IF NOT EXISTS prost_test.double_encoded AS (SELECT concat(concat(m1.lvalue, '/'), m1.rvalue) as s, t.p , concat(concat(m2.lvalue, '/'), m2.rvalue) as o, t.OTYPE FROM prost_test.triples t, prost_test.mixed m1, prost_test.mixed m2 WHERE m1.full = t.s and m2.full = t.o)");
        System.out.println("HELLO");
        spark.sql("CREATE TABLE IF NOT EXISTS prost_test.double_dictionary AS ((SELECT concat(concat(lvalue, '/'), right) as value, full as key  FROM prost_test.mixed UNION (SELECT o,o FROM prost_test.triples WHERE Otype=1)))");
        spark.sql("DROP TABLE IF EXISTS prost_test.double_encoded_full");
        System.out.println("####");
        spark.sql("CREATE TABLE IF NOT EXISTS prost_test.double_encoded AS SELECT m1.value as s,t.p, m2.value as o, t.otype FROM prost_test.double_dictionary m1, prost_test.double_dictionary m2, prost_test.triples t WHERE m1.key=t.s AND m2.key=t.o  ");
        System.out.println("####");
    }

}

    
