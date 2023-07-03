package io.conducktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {

        log.info("I am a kafka producer");


        Properties properties = new Properties();
        // connect to localhost
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to conducktor playground (remote server)
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"5WizfpKEbF577t0YgeBZFh\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI1V2l6ZnBLRWJGNTc3dDBZZ2VCWkZoIiwib3JnYW5pemF0aW9uSWQiOjc0Mzk4LCJ1c2VySWQiOjg2NTQ2LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIwNjhkNTdlNi05ZjUzLTRiNzgtOTNhZS00M2YwZGU1ZWVlMDMifX0.KszgVJZmn81TWaWEZyOY8qtZ_XS3I8MAFSB5JDrGr80\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

//        security.protocol=SASL_SSL
//        sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="5WizfpKEbF577t0YgeBZFh" password="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI1V2l6ZnBLRWJGNTc3dDBZZ2VCWkZoIiwib3JnYW5pemF0aW9uSWQiOjc0Mzk4LCJ1c2VySWQiOjg2NTQ2LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIwNjhkNTdlNi05ZjUzLTRiNzgtOTNhZS00M2YwZGU1ZWVlMDMifX0.KszgVJZmn81TWaWEZyOY8qtZ_XS3I8MAFSB5JDrGr80";
//        sasl.mechanism=PLAIN

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        // to use roundrobin partitioner
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());


        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j = 0; j< 2; j++) {
            for(int i = 0; i < 10; i++) {

                String topic = "demo_topic";
                String key = "id_" + i;
                String value = "Hello world " + i;

                // create a producer record
                // here all the msgs goes into the same partitioner: Sticky partitioner
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // executes every time a record successfully sent or an exceptions is thrown
                        if(exception == null) {
                            // the record was successfully sent
                            log.info(
                                    "Key: " + key + "|"+ " Partition: "+ metadata.partition()
                            );
                        } else {
                            log.error("Error while producing", exception);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // flush data and close the producer
        producer.flush(); // tell the producer to send all the data and block until done -- synchronous

        producer.close();
    }
}
