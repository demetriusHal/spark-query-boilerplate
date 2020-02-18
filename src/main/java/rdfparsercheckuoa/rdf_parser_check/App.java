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

    public static void main( String[] args )
    {


        App.spark = SparkSession.builder().appName("JD Word Counter").enableHiveSupport().getOrCreate();
        //App.spark.sql(queryDropTripleTable);
        //App.spark.sql(queryDropTripleTableFixed);

        runSql();


    }
    
    private static void runSql() {
        spark.sql("DROP TABLE IF EXISTS " +tableName);
        Dataset<Row> res = spark.sql(String.format("CREATE TABLE %1$s  AS SELECT DISTINCT(s) FROM prost_test.triples"));
        res = spark.sql(String.format("INSERT INTO TABLE %1$s  SELECT DISTINCT(o) FROM prost_test.triples"));
        res.show();
        // spark.sql("SELECT COUNT(DISTINCT s) FROM prost_test.vp_http___data_linkedeodata_eu_ontology_has_type").show();
        // spark.sql("SELECT COUNT(DISTINCT o) FROM prost_test.vp_http___data_linkedeodata_eu_ontology_has_type").show();



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
