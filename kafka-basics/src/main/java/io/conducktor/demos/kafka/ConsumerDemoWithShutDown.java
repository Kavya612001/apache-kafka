package io.conducktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutDown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDown.class.getSimpleName());



    public static void main(String[] args) {

        log.info("I am a kafka consumer");

        String groupId = "my-java-application";
        String topic = "demo_topic";

        Properties properties = new Properties();
        // connect to localhost
        // properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to conducktor playground (remote server)
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"5WizfpKEbF577t0YgeBZFh\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI1V2l6ZnBLRWJGNTc3dDBZZ2VCWkZoIiwib3JnYW5pemF0aW9uSWQiOjc0Mzk4LCJ1c2VySWQiOjg2NTQ2LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIwNjhkNTdlNi05ZjUzLTRiNzgtOTNhZS00M2YwZGU1ZWVlMDMifX0.KszgVJZmn81TWaWEZyOY8qtZ_XS3I8MAFSB5JDrGr80\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        // to read from the beginning
        properties.setProperty("auto.offset.reset", "earliest");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, lets exit by calling consumer.wakeup()...");
                consumer.wakeup();
                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });



        // subscribe to a topic

        try {
            consumer.subscribe(Arrays.asList(topic));

            // poll for data
            while(true) {
                // wait for 1 second to receive data from kafka
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + " | " + "Value: " + record.value());
                    log.info("Partition: " + record.partition() + " | " + "Offset: " + record.offset());
                }
            }
        } catch (WakeupException w) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer, it will also commit offsets
            log.info("The consumer is gracefully shutdown");
        }
    }
}
