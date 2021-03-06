package rdfparsercheckuoa.rdf_parser_check;

import org.apache.spark.sql.Row;


import java.io.*;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;



import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.java2d.pipe.SpanShapeRenderer;



public class App 
{
    static SparkSession spark;
    static String name_tripletable = "prost_test.tripletable1";
    static String column_name_subject = "subject";
    static String column_name_predicate = "predicate";
    static String column_name_object = "object";

    public static void main( String[] args )
    {
        final String queryDropTripleTable = String.format("DROP TABLE IF EXISTS %s", name_tripletable);
        final String queryDropTripleTableFixed = String.format("DROP TABLE IF EXISTS %s", name_tripletable);

        App.spark = SparkSession.builder().appName("JD Word Counter").enableHiveSupport().getOrCreate();
        //App.spark.sql(queryDropTripleTable);
        //App.spark.sql(queryDropTripleTableFixed);

        runSql();


    }
    
    private static void runSql() {
        spark.sql("SELECT COUNT(*) FROM prost_test.triples").show();

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
