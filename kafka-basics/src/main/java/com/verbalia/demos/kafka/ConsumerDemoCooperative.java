package com.verbalia.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private final static String CLASS_NAME = ConsumerDemoCooperative.class.getSimpleName();
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
        // If none: the consumer will throw an exception if there is no offset saved.
        // If earliest: the consumer will read from the beginning of the topic.
        // If latest: the consumer will read only new messages.
        properties.put("auto.offset.reset", "earliest");

        // Enable cooperative rebalancing.
        // This type of rebalancing is more efficient and allows for faster rebalancing.
        // In particular, it's not 'eager' and it doesn't revoke partitions until the new assignment is ready.
        properties.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // This option is for assigning static partitions to consumers.
        // Each consumer must have a unique group.instance.id.
        // properties.put("group.instance.id", "...");

        // Declare the consumer outside the try-with-resources block
        KafkaConsumer<String, String> consumer;

        // Main thread
        final Thread mainThread = Thread.currentThread();

        // Consumer
        try {
            consumer = new KafkaConsumer<>(properties);
        } catch (Exception e) {
            logger.error("Error creating KafkaConsumer: {}", e.getMessage());
            return;
        }

        // Consumer with shutdown hook
        try {
            // Shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    logger.info("Caught shutdown signal! Exiting with consumer.wakeup()...");
                    // The wakeup() method is a special method to interrupt consumer.poll()
                    // It will throw a WakeupException and allow the consumer to close gracefully
                    consumer.wakeup();

                    // Join the main thread to allow execution to continue
                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            // Subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            // Poll for new data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Partition: {}\tOffset: {}\tKey: {}\tValue: {}",
                            record.partition(), record.offset(), record.key(), record.value()
                    );
                }
            }
        } catch (WakeupException e) {
            logger.info("Received shutdown signal! Consumer closing...");
        } catch (Exception e) {
            logger.error("Unexpected exception: {}", e.getMessage());
        } finally {
            // Close the consumer. This will also commit the offsets
            consumer.close();
            logger.info("The consumer gracefully shut down...");
        }

    }
}
