package com.abaghel.examples.spark.other;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
/**
 * Word count application using Spark and Java 8
 * 
 * @author abaghel
 *
 */
public class SparkWordCount {
	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("SparkWordCount")
				.config("spark.sql.warehouse.dir", "/file:C:/temp")
				.master("local[*]") 
				.getOrCreate();
		
		JavaRDD<String> lines = spark.read().textFile("src/main/java/resources/test.txt").javaRDD();
			
		JavaPairRDD<String, Integer> counts = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
	            .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
	            .reduceByKey((x, y) ->  x +  y)
	            .sortByKey();

		counts.foreach(data -> {
		        System.out.println(data._1()+"-"+data._2());
		    });
		
		spark.stop();
	}
}
