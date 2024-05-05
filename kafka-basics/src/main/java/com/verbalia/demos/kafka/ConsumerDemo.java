package com.verbalia.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private final static String CLASS_NAME = ConsumerDemo.class.getSimpleName();
    private final static Logger logger = LoggerFactory.getLogger(CLASS_NAME);

    public static void main(String[] args) {
        logger.info("Hello, Kafka Producer!");

        String groupId = "java-application";
        String topic = "third_topic";

        // Properties
        Properties properties = new Properties();

        // Authentication
        properties.put("bootstrap.servers", "https://ample-moray-5293-us1-kafka.upstash.io:9092");
        properties.put("sasl.mechanism", "SCRAM-SHA-256");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"YW1wbGUtbW9yYXktNTI5MySu4_iP0mTwlaaR_3b7JzL4KMLscP89l3mRLJan5uk\" password=\"ZWMzOWFkNjEtZTFhZi00ZTg3LWFhNmUtNDNiNTkyNTdiNWYy\";");

        // Consumer  properties
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        properties.put("group.id", groupId);

        // Auto offset reset. Possible values: none, earliest, latest.
        // If none: the consumer will throw an exception if there are no offsets saved.
        // If earliest: the consumer will read from the beginning of the topic.
        // If latest: the consumer will read only new messages.
        properties.put("auto.offset.reset", "earliest");

        // Consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            // Subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            // Poll for new data
            while (true) {
                logger.info("Polling for new data...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                records.forEach(record -> {
                    logger.info("Key: {}\nValue: {}", record.key(), record.value());
                    logger.info("Partition: {}\nOffset: {}", record.partition(), record.offset());
                });
            }
        } catch (Exception e) {
            logger.error("Error: {}", e.getMessage());
        }

    }
}
