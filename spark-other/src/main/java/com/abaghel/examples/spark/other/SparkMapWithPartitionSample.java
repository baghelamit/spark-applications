package com.abaghel.examples.spark.other;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
/**
 * Sample for MapWithPartition in Spark
 * 
 * @author abaghel
 *
 */
public class SparkMapWithPartitionSample {
	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("SparkMapWithPartitionSample")
				.config("spark.sql.warehouse.dir", "/file:C:/temp")
				.master("local")
				.getOrCreate();

		List<String> data = Arrays.asList("one","two","three","four","five");
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		JavaRDD<String> javaRDD = sc.parallelize(data, 2);
		//JavaRDD<String> javaRDD = spark.createDataset(data, Encoders.STRING()).repartition(2).javaRDD();
		JavaRDD<String> mapPartitionsWithIndexRDD = javaRDD
				.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
					@Override
					public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
						LinkedList<String> linkedList = new LinkedList<String>();
						while (iterator.hasNext()){
								linkedList.add(Integer.toString(index) + "-" + iterator.next());
							}
						return linkedList.iterator();
					}
				}, false);
		System.out.println("MapWithPartition = " + mapPartitionsWithIndexRDD.collect());	
		spark.stop();
	}
}
