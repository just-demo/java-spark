package demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.nio.file.Paths;

import static java.util.Arrays.asList;

public class WordCounter {
    public static void main(String[] args) {
        String inFile = Paths.get("data").resolve("in.txt").toAbsolutePath().toString();
        SparkConf sparkConf = new SparkConf().setAppName("WordCounter");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.textFile(inFile, 1)
                .flatMap(line -> asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .collect()
                .forEach(tuple -> System.out.println(tuple._1() + ": " + tuple._2()));
        sparkContext.stop();
    }
}
