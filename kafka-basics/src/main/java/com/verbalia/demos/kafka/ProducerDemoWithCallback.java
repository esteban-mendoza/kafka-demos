package com.verbalia.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * <p>Kafka Producer with Callback. This class sends 10 batches of 30 messages each to a Kafka topic
 *  with a wait of 300 ms between each message.</p>
 * <p>This producer uses the default partitioner to distribute the messages across the partitions.
 * This is the Strictly Uniform Sticky Partitioner, according to <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-794%3A+Strictly+Uniform+Sticky+Partitioner#KIP794:StrictlyUniformStickyPartitioner-NewDefaultPartitioner(allsettingsaredefault)">KIP-794</a></p>
 */
public class ProducerDemoWithCallback {

    private final static String CLASS_NAME = ProducerDemoWithCallback.class.getSimpleName();
    private final static Logger logger = LoggerFactory.getLogger(CLASS_NAME);

    public static void main(String[] args) {
        logger.info("Hello, I'm a Kafka Producer!");

        // Properties
        Properties properties = new Properties();

        // Authentication
        properties.put("bootstrap.servers", "https://ample-moray-5293-us1-kafka.upstash.io:9092");
        properties.put("sasl.mechanism", "SCRAM-SHA-256");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"YW1wbGUtbW9yYXktNTI5MySu4_iP0mTwlaaR_3b7JzL4KMLscP89l3mRLJan5uk\" password=\"ZWMzOWFkNjEtZTFhZi00ZTg3LWFhNmUtNDNiNTkyNTdiNWYy\";");

        // Producer properties
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        properties.put("batch.size", 400);

        // Producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            // 10 batches
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 30; j++) {
                    // Producer record
                    String message = "Batch" + i + ". Message " + j + " from " + CLASS_NAME + " with callback";
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>("third_topic", message);

                    // Send data. Asynchronous operation
                    producer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            // executes whenever a record is successfully sent or an exception is thrown
                            if (exception == null) {
                                logger.info("Received new metadata. \n" +
                                        "Topic: {}\n" +
                                        "Partition: {}\n" +
                                        "Offset: {}\n" +
                                        "Timestamp: {}", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                            } else {
                                logger.error("Error while producing: {}", exception.getMessage());
                            }
                        }
                    });

                    // Sleep for 300 ms
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        logger.error("Interrupted error: {}", e.getMessage());
                    }

                }
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
