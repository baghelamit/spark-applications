package com.abaghel.examples.spark.other;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
/**
 * Partition a Dataset by Key and save each as separate CSV file
 * 
 * @author abaghel
 *
 */
public class SparkPartitionByCSV {
	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("SparkPartitionByCSV")
				.config("spark.sql.warehouse.dir", "/file:C:/temp")
				.master("local[*]") 
				.getOrCreate();
		//input data
		List<Tuple2<String,String>> inputList = new ArrayList<Tuple2<String,String>>();
		inputList.add(new Tuple2<String,String>("A", "v1"));
		inputList.add(new Tuple2<String,String>("A", "v2"));
		inputList.add(new Tuple2<String,String>("B", "v3"));
		inputList.add(new Tuple2<String,String>("A", "v4"));
		//dataset
		Dataset<Row> dataWrite = spark.createDataset(inputList, Encoders.tuple(Encoders.STRING(), Encoders.STRING())).toDF("key","value");
		dataWrite.show();	
		//write and read
		dataWrite.write().mode(SaveMode.Overwrite).partitionBy("key").format("csv").option("header", "true").save("c:\\temp\\data");
		Dataset<Row> dataRead = spark.read().format("csv").option("header", "true").load("c:\\temp\\data");
		dataRead.show();
		//stop
		spark.stop();
	}
}
