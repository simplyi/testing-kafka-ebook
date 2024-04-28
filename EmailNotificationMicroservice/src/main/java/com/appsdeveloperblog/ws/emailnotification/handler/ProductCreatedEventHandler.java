package com.appsdeveloperblog.ws.emailnotification.handler;

import com.appsdeveloperblog.ws.core.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	@Transactional
	@KafkaHandler
	public void handle(@Payload ProductCreatedEvent productCreatedEvent, @Header("messageId") String messageId,
			@Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {
		LOGGER.info("Received a new event: " + productCreatedEvent.getTitle());
	}
}
