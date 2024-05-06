package com.verbalia.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;

public class ProducerDemoBlocked {

    private final static String CLASS_NAME = ProducerDemoBlocked.class.getSimpleName();
    private final static Logger logger = LoggerFactory.getLogger(CLASS_NAME);

    public static void main(String[] args) {
        logger.info("Hello, Kafka Producer!");

        // Properties
        Properties properties = new Properties();

        // Authentication
        properties.put("bootstrap.servers", "https://ample-moray-5293-us1-kafka.upstash.io:9092");
        properties.put("sasl.mechanism", "SCRAM-SHA-256");
        properties.put("security.protocol", "SASL_SSL");

        String username = System.getenv("KAFKA_USERNAME");
        String password = System.getenv("KAFKA_PASSWORD");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";");

        // Producer properties
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        // Producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            // Producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("third_topic", "Hello, from the blocked Java producer!");

            // Send data. Asynchronous operation
            Future<RecordMetadata> futureMetadata = producer.send(record);

            // Block the main thread until the send operation is complete
            try {
                RecordMetadata metadata = futureMetadata.get();
                logger.info("Received new metadata. \n" +
                        "Topic: {}\n" +
                        "Partition: {}\n" +
                        "Offset: {}\n" +
                        "Timestamp: {}", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
            } catch (CancellationException e) {
                logger.error("Cancellation error: {}", e.getMessage());
            } catch (InterruptedException e) {
                logger.error("Interrupted error: {}", e.getMessage());
            }
            // Make producer send all buffered records and blocks until all records have been sent.
            // Synchronous operation
            producer.flush();

            // Flush and close producer
            producer.close();
        } catch (Exception e) {
            logger.error("Error: {}", e.getMessage());
        }

    }
}
