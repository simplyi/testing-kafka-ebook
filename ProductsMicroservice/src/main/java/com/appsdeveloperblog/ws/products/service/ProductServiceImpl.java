package com.appsdeveloperblog.ws.products.service;

import java.util.UUID;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import com.appsdeveloperblog.ws.core.ProductCreatedEvent;
import com.appsdeveloperblog.ws.products.rest.CreateProductRestModel;

@Service
public class ProductServiceImpl implements ProductService {
	
	KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
	private final Logger LOGGER  = LoggerFactory.getLogger(this.getClass());
	
	public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@Override
	public String createProduct(CreateProductRestModel productRestModel) throws Exception {
		String productId = UUID.randomUUID().toString();

		ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId,
				productRestModel.getTitle(), productRestModel.getPrice(), 
				productRestModel.getQuantity());

		ProducerRecord<String, ProductCreatedEvent> record = new ProducerRecord<>(
				"product-created-events-topic",
				productId,
				productCreatedEvent);
		record.headers().add("messageId", UUID.randomUUID().toString().getBytes());
		
		SendResult<String, ProductCreatedEvent> result = 
				kafkaTemplate.send(record).get();
		
		LOGGER.debug("Partition: " + result.getRecordMetadata().partition());
		LOGGER.debug("Topic: " + result.getRecordMetadata().topic());
		LOGGER.debug("Offset: " + result.getRecordMetadata().offset());

		return productId;
	}

}
