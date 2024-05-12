package io.demo.kafka;

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
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());
    private static  final String TOPIC_NAME = "java_topic";
    public static void main(String[] args) {
        String groupId = "my-java-application";

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        //strategy for static assignments
        //properties.setProperty("group.instance.id", "");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // get a reference to main thread
        final Thread mainThread = Thread.currentThread();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Detected a shutdown, consumer.wakeup() getting called");
            kafkaConsumer.wakeup();

            // join the main thread to allow execution of code in main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        //subscribe to the topic
        kafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));

        //poll for data
        try{
            while (true){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record: consumerRecords){
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }

        }
        catch (WakeupException e){
            logger.info("Consumer shutting down");
        }
        catch (Exception e){
            logger.error("Unexpected exception occurred");
        }
        finally {
            kafkaConsumer.close();
            logger.info("Consumer gracefully shutdown");
        }

    }
}
