package com.miu.cs523.sparkconsumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.miu.cs523.sparkconsumer.service.SparkKafkaConsumerService;

@SpringBootApplication
public class SparkConsumerApplication implements CommandLineRunner {

	@Autowired
	private SparkKafkaConsumerService sparkKafkaConsumerService;
	
	public static void main(String[] args) {
		SpringApplication.run(SparkConsumerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		sparkKafkaConsumerService.run();
		
	}

}
