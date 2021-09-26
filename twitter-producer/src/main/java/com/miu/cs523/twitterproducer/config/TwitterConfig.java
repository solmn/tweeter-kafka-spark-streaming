package com.miu.cs523.twitterproducer.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.social.twitter.api.impl.TwitterTemplate;

@Configuration
public class TwitterConfig {
	
	@Value("${spring.social.twitter.consumerAPIKey}")
	private String consumerAPIKey;
	
	@Value("${spring.social.twitter.consumerAPISecret}")
	private String consumerAPISecret;
	
	@Value("${spring.social.twitter.accessToken}")
	private String accessToken;
	
	@Value("${spring.social.twitter.accessTokenSecrete}")
	private String accessTokenSecrete;
	
	@Bean
	public TwitterTemplate twitterTemplate() {
		return new TwitterTemplate(this.consumerAPIKey, this.consumerAPISecret, this.accessToken, this.accessTokenSecrete);
	}
	
	

}
