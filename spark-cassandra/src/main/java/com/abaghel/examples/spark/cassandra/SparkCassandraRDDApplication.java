package com.abaghel.examples.spark.cassandra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import scala.Tuple4;
/**
 * Sample application to save data in to cassandra table and read back using spark.
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

		//Create
		List<Tuple4<String, String, String, String>> attributes = new ArrayList<Tuple4<String, String, String, String>>();
		Tuple4<String, String, String, String> t4 = new Tuple4("att-1", "att-2", "att-3", "att-4");
		attributes.add(t4);
		//user-1
		UserData ud = new UserData();
		ud.setId(UUID.randomUUID());
		ud.setUsername("Amit");
		ud.setEmail("amit@yahoo.com");
		ud.setAttributes(attributes);
		//user-2
		UserData ud1 = new UserData();
		ud1.setId(UUID.randomUUID());
		ud1.setUsername("Mat");
		ud1.setEmail("mat@yahoo.com");
		ud1.setAttributes(attributes);

		List<UserData> users = Arrays.asList(ud,ud1);
		JavaRDD<UserData> rdd = sc.parallelize(users);
		javaFunctions(rdd).writerBuilder("sparkcassandrakeyspace", "userdata", CassandraJavaUtil.mapToRow(UserData.class)).saveToCassandra();

		//Read
		JavaRDD<UserData> resultsRDD = javaFunctions(sc).cassandraTable("sparkcassandrakeyspace", "userdata",CassandraJavaUtil.mapRowTo(UserData.class));
		resultsRDD.foreach(data -> {
			System.out.println(data.id);
			System.out.println(data.username);
			System.out.println(data.email);
			System.out.println(data.attributes);
		});
		
		sc.stop();
	}

}
