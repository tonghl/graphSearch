package mob;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkTest {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("dsfs");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("ttttt");
        JavaRDD<String> rdd = sc.parallelize(list);
    }
}
