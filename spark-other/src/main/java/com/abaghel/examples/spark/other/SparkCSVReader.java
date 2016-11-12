package com.abaghel.examples.spark.other;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
/**
 * Application to read and write CSV file using Spark
 * 
 * @author abaghel
 *
 */
public class SparkCSVReader {

	public static void main(String[] args) {

		SparkSession spark = SparkSession
				.builder()
				.appName("SparkCSVReader")
				.config("spark.sql.warehouse.dir", "/file:C:/temp")
				.master("local")
				.getOrCreate();

		Dataset<Row> ds = spark
				.sqlContext()
				.read()
				.format("com.databricks.spark.csv")
				.option("header", "true")
				.load("src/main/java/resources/test.csv");
		
		ds.show();
		
		ds.toDF().write()
		.format("com.databricks.spark.csv")
		.mode(SaveMode.Overwrite)
		.save("src/main/java/resources/test1.csv");

	}

}
