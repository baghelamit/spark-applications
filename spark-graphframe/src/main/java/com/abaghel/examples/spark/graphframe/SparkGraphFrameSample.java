package com.abaghel.examples.spark.graphframe;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;
/**
 * Sample application shows how to create a GraphFrame, query it, and run the PageRank algorithm.
 * 
 * @author abaghel
 *
 */
public class SparkGraphFrameSample {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("SparkGraphFrameSample")
				.config("spark.sql.warehouse.dir", "/file:C:/temp")
				.master("local[2]")
				.getOrCreate();
		
		//Create a Vertex DataFrame with unique ID column "id"
		List<User> uList = new ArrayList<User>() {
			{
				add(new User("a", "Alice", 34));
				add(new User("b", "Bob", 36));
				add(new User("c", "Charlie", 30));
			}
		};
		
		Dataset<Row> verDF = spark.createDataFrame(uList, User.class);
		
		//Create an Edge DataFrame with "src" and "dst" columns
		List<Relation> rList = new ArrayList<Relation>() {
			{
				add(new Relation("a", "b", "friend"));
				add(new Relation("b", "c", "follow"));
				add(new Relation("c", "b", "follow"));
			}
		};

		Dataset<Row> edgDF = spark.createDataFrame(rList, Relation.class);
		
		//Create a GraphFrame
		GraphFrame gFrame = new GraphFrame(verDF, edgDF);
		//Get in-degree of each vertex.
		gFrame.inDegrees().show();
		//Count the number of "follow" connections in the graph.
		long count = gFrame.edges().filter("relationship = 'follow'").count();
		//Run PageRank algorithm, and show results.
		PageRank pRank = gFrame.pageRank().resetProbability(0.01).maxIter(5);
		pRank.run().vertices().select("id", "pagerank").show();
		
		//stop
		spark.stop();

	}

}
