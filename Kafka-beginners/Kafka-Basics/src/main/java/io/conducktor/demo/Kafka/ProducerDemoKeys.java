package io.conducktor.demo.Kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProdcuerDemo.class.getSimpleName());

    public static void main(String[] args) {

        //create properties
        Properties properties = new Properties();

        //connect to localhost
        //properties.setProperty("bootstrap.server","127.0.0.1:9092");

        //connect to Conduktor platform
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"4e6xurW4plU1y3AukyROPh\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI0ZTZ4dXJXNHBsVTF5M0F1a3lST1BoIiwib3JnYW5pemF0aW9uSWQiOjczMDMyLCJ1c2VySWQiOjg0ODk0LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIyZWYxYzE2Ny1lN2M3LTRmMmMtOTk3Yi04NWFhYzZiNzA0YjEifX0.rMdPlgEdilQ0v644CBpdJhp-vEfJVCauTrThlusBXd4\";");
        properties.setProperty("sasl.mechanism", "PLAIN");


        //SET producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");


        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j = 0; j < 2; j++)
        {
            for (int i = 0; i < 10; i++) {

                String topic = "demo_java_topic";
                String key = "id_" + i;
                String value = "Hello from java client " + i;

                //Create a Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                //Send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //Execute everytime a record successfully sent or exception occurs
                        if (e == null) {
                            //the record was successfully sent
                            log.info("key - " + key + " | Partition: " + metadata.partition());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //tell producer to send all data and block until done - synchronous
        producer.flush();
        //flush and exit
        producer.close();
    }
}
