package com.abaghel.examples.spark.other;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
/**
 * Word Filter application using Spark and Java 8
 * 
 * @author abaghel
 *
 */
public class SparkWordFilter {
	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("SparkWordFilter")
				.config("spark.sql.warehouse.dir", "/file:C:/temp")
				.master("local[*]") 
				.getOrCreate();
		
		JavaRDD<String> lines = spark.read().textFile("src/main/java/resources/test1.txt").javaRDD();
		//Using Dataset
		Dataset<String> ds = spark.createDataset(lines.collect(), Encoders.STRING());
		ds.show();
		JavaRDD<String> counts = ds.as(Encoders.STRING()).toJavaRDD().flatMap(line -> Arrays.asList(line.split(" ")).iterator()).filter(word -> word.startsWith("#"));			
		
		//Using RDD
		//JavaRDD<String> counts = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).filter(word -> word.startsWith("#"));
		
		counts.foreach(data -> {
		        System.out.println(data);
		    });
		
		spark.stop();
	}
}
