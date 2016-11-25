package com.abaghel.examples.spark.other;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Read CSV with Date Format and Write
 * 
 * @author abaghel
 *
 */
public class SparkCSVDataset {

	public static void main(String[] args) {
		//SparkSession
		SparkSession spark = SparkSession
				.builder()
				.appName("SparkCSVDataset")
				.config("spark.sql.warehouse.dir", "/file:C:/temp")
				.master("local")
				.getOrCreate();
		//Schema
		StructType schema = new StructType(new StructField[] { 
						new StructField("Id", DataTypes.IntegerType, true, Metadata.empty()),
						new StructField("Name", DataTypes.StringType, true, Metadata.empty()),
						new StructField("DOB", DataTypes.DateType, true, Metadata.empty()) });
		//Read
		Dataset<Row> ds = spark.read().format("csv").option("dateFormat", "MM/dd/yyyy").schema(schema).option("header", "true").option("delimiter", ",").load("src/main/java/resources/test.csv");
		ds.show();
		ds.printSchema();
		//Write
		ds.write().mode(SaveMode.Overwrite).format("csv").option("dateFormat", "yyyy-MM-dd").option("header", "true").save("src/main/java/resources/testOut.csv");
	}

}
