package com.abaghel.examples.spark.other;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;
/**
 *Sample Kafka Streaming with Spark and Conversion of Dstream into Dataframe
 * 
 * @author abaghel
 *
 */

public class SparkKafkaStreaming {
	 public static void main(String[] args) throws Exception {
		 //Create SparkConf	
	     SparkConf conf = new SparkConf()
				 .setAppName("KafkaSparkStreaming")
				 .setMaster("local");
	     
         //batch interval of 5 seconds for incoming stream		 
		 JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));	
		 //add check point directory
		 jssc.checkpoint("/tmp/iot-streaming-data");		 
		 //read and set Kafka properties
		 Map<String, String> kafkaParams = new HashMap<String, String>();
		 kafkaParams.put("zookeeper.connect", "localhost:2181");
		 kafkaParams.put("metadata.broker.list", "localhost:9092");
		 Set<String> topicsSet = new HashSet<String>();
		 topicsSet.add("iot-data-event");

        //Create an input DStream for Receiving data from socket
        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(jssc,
                String.class, 
                String.class, 
                StringDecoder.class, 
                StringDecoder.class, 
                kafkaParams, topicsSet);
        
        //Create JavaDStream<String>
        JavaDStream<String> msgDataStream = directKafkaStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
              return tuple2._2();
            }
          });
        //Create JavaRDD<Row>
        msgDataStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
        	  @Override
        	  public void call(JavaRDD<String> rdd) { 
        		  JavaRDD<Row> rowRDD = rdd.map(new Function<String, Row>() {
        	          @Override
        	          public Row call(String msg) {
        	            Row row = RowFactory.create(msg);
        	            return row;
        	          }
        	        });
        //Create Schema		  
        StructType schema = DataTypes.createStructType(new StructField[] {DataTypes.createStructField("Message", DataTypes.StringType, true)});
        //Get Spark 2.0 session		  
        SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
        Dataset<Row> msgDataFrame = spark.createDataFrame(rowRDD, schema);
        msgDataFrame.show();
        	  }
        });
        //start context
        jssc.start();            
        jssc.awaitTermination();  
    }
    
}

class JavaSparkSessionSingleton {
	  private static transient SparkSession instance = null;
	  public static SparkSession getInstance(SparkConf sparkConf) {
	    if (instance == null) {
	      instance = SparkSession
	        .builder()
	        .config(sparkConf)
	        .getOrCreate();
	    }
	    return instance;
	  }
	}
