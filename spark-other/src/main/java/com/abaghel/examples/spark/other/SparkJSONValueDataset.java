package com.abaghel.examples.spark.other;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
/**
 * Sample to convert json String in Dataset to Dataset having individual columns
 * 
 * @author abaghel
 *
 */
public class SparkJSONValueDataset {
	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("SparkJSONValueDataset")
				.config("spark.sql.warehouse.dir", "/file:C:/temp")
				.master("local")
				.getOrCreate();
		
		List<String> data = Arrays.asList("{\"Id\":\"aaff1-22bac\",\"Name\":\"Jhon Doe\",\"Age\":\"35\"}");
		
		/*//Prepare data Dataset<Row> and then convert to Dataset<String>
		Dataset<Row> df = spark.createDataset(data, Encoders.STRING()).toDF().withColumnRenamed("_1", "value");
		Dataset<String> df1 = df.as(Encoders.STRING());*/
		
		//If we have Dataset<String>
		Dataset<String> df1 = spark.createDataset(data, Encoders.STRING());
		df1.show();
		//json string df to df
		Dataset<Row> df2 = spark.read().json(df1.javaRDD());
		df2.show();
		spark.stop();
	}
}
