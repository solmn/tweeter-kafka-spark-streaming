package com.miu.cs523.twitterproducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.miu.cs523.twitterproducer.service.TwitterKafkaStreamingService;

@SpringBootApplication
public class TwitterProducerApplication implements CommandLineRunner{
	
	@Autowired
	private TwitterKafkaStreamingService twitterKafkaStreamingService;

	public static void main(String[] args) {
		SpringApplication.run(TwitterProducerApplication.class, args);
	}
	
	@Override
	public void run(String... strings) throws Exception {
		
		twitterKafkaStreamingService.run();
	}

}
