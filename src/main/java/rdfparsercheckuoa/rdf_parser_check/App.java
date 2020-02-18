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


import java.util.Arrays;




public class App 
{
    static SparkSession spark;
    static String name_tripletable = "prost_test.triples";
    static String column_name_subject = "subject";
    static String column_name_predicate = "predicate";
    static String column_name_object = "object";
    static String tableName = "distinct_literals";
    static String tableName1 = "distinct_literals_fixed";
    static String tableName2 = "dict_final";
    static String triples_zipped = "triples_zipped";

    public static void main( String[] args )
    {


        App.spark = SparkSession.builder().appName("JD Word Counter").enableHiveSupport().getOrCreate();
        //App.spark.sql(queryDropTripleTable);
        //App.spark.sql(queryDropTripleTableFixed);

        showTuples(args[0]);


    }

    private static void encode() {
        Dataset<Row> res = spark.sql("SELECT s FROM prost_test"+tableName1);
    }
    
    private static void runSql() {
        spark.sql("DROP TABLE IF EXISTS prost_test." +tableName);
        Dataset<Row> res = spark.sql(String.format("CREATE TABLE prost_test.%1$s  AS SELECT DISTINCT(s) FROM prost_test.triples", tableName));
        res = spark.sql(String.format("INSERT INTO TABLE prost_test.%1$s  SELECT DISTINCT(o) FROM prost_test.triples", tableName));
        res.show();
        // spark.sql("SELECT COUNT(DISTINCT s) FROM prost_test.vp_http___data_linkedeodata_eu_ontology_has_type").show();
        // spark.sql("SELECT COUNT(DISTINCT o) FROM prost_test.vp_http___data_linkedeodata_eu_ontology_has_type").show();


    }
    private static void showTuples(String relation) {
        spark.sql("SELECT * from prost_test."+relation).show(1000);


    }

    private static void createDistinctTable() {
        spark.sql(String.format("CREATE TABLE prost_test.%1$s  AS SELECT DISTINCT(s) FROM prost_test.%2$s", tableName1, tableName));
    }

    private static void countTuples(String relation) {
        spark.sql(String.format("SELECT COUNT(s) FROM prost_test.%1$s WHERE char_length(s) > 16", tableName1)).show();
    }

    private static void addRowNumber() {
        spark.sql(String.format("CREATE TABLE prost_test.%1$s  AS SELECT s as key, concat('dd', (row_number() over (order by s))) value FROM prost_test.%2$s", tableName2, tableName1));

    }

    private static void createTriplesZipped() {
        spark.sql("DROP TABLE IF EXISTS prost_test." +triples_zipped);
        spark.sql(String.format("CREATE TABLE prost_test.%1$s as SELECT t1.value as s,t.p,t2.value as o FROM prost_test.dict_final t1, prost_test.dict_final t2, prost_test.triples t WHERE t1.key=t.s AND t2.key=t.o",triples_zipped));
    }


    private static void wordCount(String fileName, String outfile) {

        SparkConf sparkConf = new SparkConf().setAppName("JD Word Counter");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> inputFile = sparkContext.textFile(fileName);
        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
        JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);
        countData.saveAsTextFile(outfile);
    }
    

    

    
}
