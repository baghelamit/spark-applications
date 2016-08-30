package com.abaghel.examples.spark.cassandra;

import java.util.HashMap;

import org.apache.spark.sql.SparkSession;
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
			      .appName("SparkCassandraDataFrameApplication")
			      .config("spark.sql.warehouse.dir", "/file:C:/temp")
			      .config("spark.cassandra.connection.host", "127.0.0.1")
				  .config("spark.cassandra.connection.port", "9042")
			      .master("local[2]")
			      .getOrCreate();
		
		//Read
        spark.read().format("org.apache.spark.sql.cassandra")
				.options(new HashMap<String, String>() {
					{
						put("keyspace", "sparkcassandrakeyspace");
						put("table", "userdata");
					}
				}).load().show();
        
        spark.stop();

	}
		
}
