package com.abaghel.examples.spark.couchbase;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.spark.japi.CouchbaseDocumentRDD;
import com.couchbase.spark.japi.CouchbaseSparkContext;

/**
 * Spark Sample to save JSON document to Couchbase and retrieve the created document.
 * 
 * @author abaghel
 *
 */
public class SparkCouchbaseApplication {

	public static void main(String[] args) {
		// JavaSparkContext
		SparkSession spark = SparkSession
				.builder()
				.appName("SparkCouchbaseApplication")
				.master("local[*]") 
				.config("spark.couchbase.nodes", "127.0.0.1")
				.config("spark.couchbase.bucket.travel-sample", "") 
				.getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		// CouchbaseSparkContext
		CouchbaseSparkContext csc = CouchbaseSparkContext.couchbaseContext(jsc);
		// Create and save JsonDocument
		JsonDocument docOne = JsonDocument.create("docOne", JsonObject.create().put("new", "doc-content"));
		JavaRDD<JsonDocument> jRDD = jsc.parallelize(Arrays.asList(docOne));
		CouchbaseDocumentRDD<JsonDocument> cbRDD = CouchbaseDocumentRDD.couchbaseDocumentRDD(jRDD);
		cbRDD.saveToCouchbase();
		// fetch JsonDocument
		List<JsonDocument> doc = csc.couchbaseGet(Arrays.asList("docOne")).collect();
		System.out.println(doc);
	}

}
