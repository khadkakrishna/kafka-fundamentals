package io.demo.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    private static  final String TOPIC_NAME = "java_topic";
    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Following code will create topic if it didn't exist
        try (AdminClient adminClient = AdminClient.create(properties)) {
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(true); // includes internal topics such as __consumer_offsets
            ListTopicsResult topics = adminClient.listTopics(options);
            Set<String> currentTopicList = topics.names().get();
            boolean exists = currentTopicList.stream().anyMatch(topicName -> topicName.equalsIgnoreCase(TOPIC_NAME));

            if(!exists){
                NewTopic newTopic = new NewTopic(TOPIC_NAME, 3, (short)1); //new NewTopic(topicName, numPartitions, replicationFactor)
                ArrayList<NewTopic> topicArrayList = new ArrayList<>();
                topicArrayList.add(newTopic);
                adminClient.createTopics(topicArrayList);
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, "I am kafka producer");

        //send is asynchronous
        kafkaProducer.send(producerRecord);

        //sync operation: send all data and block until done
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
