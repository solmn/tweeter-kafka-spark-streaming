package com.miu.cs523.sparkconsumer.service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.streaming.Durations;

import com.miu.cs523.sparkconsumer.util.*;

import scala.Tuple2;

@Service
public class SparkKafkaConsumerService {
     @Value("${spring.kafka.consumer.bootstrap-servers}")
     private String bootstrapServers;
     
     @Value("${spring.kafka.consumer.group-id}")
     private String groupId;
     
     @Value("${spring.kafka.consumer.auto-offset-reset}")
     private String autoOffset;
     
     @Value("${kafkaTopic}")
     private String kafkaTopic;
     
     @Autowired
     HBaseService hbaseService;
     
     private static final String TABLE_NAME = "tweets";
     Map<String, Object> kafkaParams = new HashMap<>();
     Collection<String> topics = Arrays.asList("tweets");
     
     public void initKafkaParams() {
         kafkaParams.put("bootstrap.servers", "localhost:9092");
         kafkaParams.put("key.deserializer", StringDeserializer.class);
         kafkaParams.put("value.deserializer", StringDeserializer.class);
         kafkaParams.put("group.id", "group_id");
         kafkaParams.put("auto.offset.reset", "earliest");
     }
     
     public SparkKafkaConsumerService() {
    	 this.initKafkaParams();
     }
     
     
     public void run() throws IOException {
    	 
    	 /**
    	  *  Creating Streaming context
    	  *  Spark  collects 10 seconds worth of data to process.
    	  * **/
         JavaStreamingContext streamingContext = new JavaStreamingContext(
        		 new SparkConf().setAppName("Tweets")
        		                .setMaster("local[*]"),
        		 Durations.seconds(10));
         
        /**
         * Creating Hbase connection 
         * **/
        Configuration config = HBaseConfiguration.create();
 		config.set("hbase.zookeeper.quorum", "localhost");
 		config.set("hbase.zookeeper.property.clientPort", "2181");
		Connection connection = ConnectionFactory.createConnection(config);
		
        /**
         * Creating the Hbase table to store spark streaming processing result
         * */
 		hbaseService.createTable(connection);
 		
        JavaInputDStream<ConsumerRecord<String, String>> stream =
        		  KafkaUtils.createDirectStream(
        		    streamingContext,
        		    LocationStrategies.PreferConsistent(),
        		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        		  );
         
         stream.map(tweet -> tweet.value())
         		.flatMap(tweet -> HashTagsUtils.hashTagsFromTweet(tweet))
	            .mapToPair(hashTag -> new Tuple2<>(hashTag, 1)) //// Map each hashtag to a key/value pair of (hashtag, 1)
	            //so we can count them up by adding up the values
	            .reduceByKeyAndWindow((x, y) -> x  + y, Durations.seconds(120), Durations.seconds(10))
	            //capturing all the tweets according to the window duration.
	            //We will be fetching previous 120-seconds tweets after every 10 second
	            .mapToPair(tweet -> tweet.swap())
                .foreachRDD(rrdd -> {
                     rrdd.sortByKey(false)
                          .collect()
                          .forEach( record -> {
                                      System.out.println(String.format(":::::::::::::::::::::::::: %s (%d)", record._2, record._1));
				                         try {
											hbaseService.saveData(connection, TABLE_NAME, record._2, record._1.toString());
										} catch (IOException e) {
											e.printStackTrace();
										}
                          });

                 });
         
//         streamingContext.checkpoint("/Users/solmn/checkpoint");
         
         streamingContext.start();
         try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

         

     }
     
     

}
