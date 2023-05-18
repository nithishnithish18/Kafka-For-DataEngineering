package io.conducktor.demo.Kafka;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProdcuerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProdcuerDemo.class.getSimpleName());

    public static void main(String[] args) {

        //create properties
        Properties properties = new Properties();

        //connect to localhost
        //properties.setProperty("bootstrap.server","127.0.0.1:9092");

        //connect to Conduktor platform
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"4e6xurW4plU1y3AukyROPh\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI0ZTZ4dXJXNHBsVTF5M0F1a3lST1BoIiwib3JnYW5pemF0aW9uSWQiOjczMDMyLCJ1c2VySWQiOjg0ODk0LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIyZWYxYzE2Ny1lN2M3LTRmMmMtOTk3Yi04NWFhYzZiNzA0YjEifX0.rMdPlgEdilQ0v644CBpdJhp-vEfJVCauTrThlusBXd4\";");
        properties.setProperty("sasl.mechanism","PLAIN");


        //SET producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Create a Producer Record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_topic","Hello from Java client");

        //Send data
        producer.send(producerRecord);

        //tell producer to send all data and block until done - synchronous
        producer.flush();

        //flush and exit
        producer.close();


    }
}
