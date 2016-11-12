package com.abaghel.examples.spark.cassandra;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
/**
 * Sample application to save data in to Cassandra table and read back using spark.
 * This sample uses spark's JavaRDD
 * 
 * @author abaghel
 *
 */
public class SparkCassandraRDDApplication {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("SparkCassandraRDDApplication")
				.setMaster("local[2]")
				.set("spark.cassandra.connection.host", "127.0.0.1")
				.set("spark.cassandra.connection.port", "9042");

		JavaSparkContext sc = new JavaSparkContext(conf);	

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

		List<User> users = Arrays.asList(ud,ud1);
		JavaRDD<User> rdd = sc.parallelize(users);
		javaFunctions(rdd).writerBuilder("sparkcassandrakeyspace", "user", CassandraJavaUtil.mapToRow(User.class)).saveToCassandra();

		//Read
		JavaRDD<User> resultsRDD = javaFunctions(sc).cassandraTable("sparkcassandrakeyspace", "user",CassandraJavaUtil.mapRowTo(User.class));
		resultsRDD.foreach(data -> {
			System.out.println(data.id);
			System.out.println(data.username);
			System.out.println(data.email);
		});

		sc.stop();
	}

}
