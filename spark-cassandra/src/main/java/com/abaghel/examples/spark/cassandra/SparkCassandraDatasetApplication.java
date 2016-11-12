package com.abaghel.examples.spark.cassandra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple4;
/**
 * Sample application to read cassandra table using spark Dataset.
 * 
 * @author abaghel
 *
 */
public class SparkCassandraDatasetApplication {
	public static void main(String[] args) {
		 SparkSession spark = SparkSession
			      .builder()
			      .appName("SparkCassandraDatasetApplication")
			      .config("spark.sql.warehouse.dir", "/file:C:/temp")
			      .config("spark.cassandra.connection.host", "127.0.0.1")
				  .config("spark.cassandra.connection.port", "9042")
			      .master("local[2]")
			      .getOrCreate();
		 
			//user-1
			User ud = new User();
			ud.setId(UUID.randomUUID().toString());
			ud.setUsername("Amit");
			ud.setEmail("amit@yahoo.com");
			//user-2
			User ud1 = new User();
			ud1.setId(UUID.randomUUID().toString());
			ud1.setUsername("Mat");
			ud1.setEmail("mat@yahoo.com");
			//write
			List<User> users = Arrays.asList(ud,ud1);
			Dataset<User> datasetWrite = spark.createDataset(users, Encoders.bean(User.class));
			datasetWrite.write().format("org.apache.spark.sql.cassandra")
						.options(new HashMap<String, String>() {
							{
								put("keyspace", "sparkcassandrakeyspace");
								put("table", "user");
							}
						}).save();
			//Read
		    Dataset<Row> dataset = spark.read().format("org.apache.spark.sql.cassandra")
				.options(new HashMap<String, String>() {
					{
						put("keyspace", "sparkcassandrakeyspace");
						put("table", "user");
					}
				}).load();
		 
		 dataset.show();       
         spark.stop();
	}
		
}
