package com.miu.cs523.twitterproducer.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.social.twitter.api.StreamDeleteEvent;
import org.springframework.social.twitter.api.StreamListener;
import org.springframework.social.twitter.api.StreamWarningEvent;
import org.springframework.social.twitter.api.Tweet;
import org.springframework.social.twitter.api.Twitter;
import org.springframework.stereotype.Service;
import com.miu.cs523.twitterproducer.util.*;

@Service
public class TwitterKafkaStreamingService {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
     @Value("${kafkaTopic}")
     private String kafkaTopic;
     
     
     @Autowired
     private Twitter twitter;
     
     @Autowired
     private KafkaProducerService kafkaService;
     
     
     
     public void run() {
    	 List<StreamListener> listeners = new ArrayList<StreamListener>();
    	 
    	 StreamListener streamListeners = new StreamListener() {

			@Override
			public void onTweet(Tweet tweet) {
				String lang = tweet.getLanguageCode();
				String text = tweet.getText();
				
				
				//filter non-English tweets:
                if (!"en".equals(lang)) {
                    return;
                }

                Iterator<String> hashTags = HashTagsUtils.hashTagsFromTweet(text);

                // filter tweets without hashTags:
                if (!hashTags.hasNext()) {
                    return;
                }
                while(hashTags.hasNext()) {
                	System.out.println("::::::::TAG:::::::" + hashTags.next());
                }
                
              //Send tweet to Kafka topic
                //logger.info("User '{}', Tweeted : {}, from ; {}", tweet.getUser().getName() , tweet.getText(), tweet.getUser().getLocation());
                kafkaService.send(kafkaTopic, tweet.getText());
//                System.out.println("::::::::::::::::::::" + tweet.getText());
				
			}

			@Override
			public void onDelete(StreamDeleteEvent deleteEvent) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onLimit(int numberOfLimitedTweets) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void onWarning(StreamWarningEvent warningEvent) {
				// TODO Auto-generated method stub
				
			}
     
     };
     
     listeners.add(streamListeners);
     twitter.streamingOperations().sample(listeners);
     
   }
}
