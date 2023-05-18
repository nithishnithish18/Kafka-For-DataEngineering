package io.conducktor.demo.Kafka;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class ConsumerDemoWithShutdown {

    private static final Logger log= LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am kafka consumer!");

        String groupId = "my-java-application_1";
        String topic = "demo_java_topic";

        // create Producer Properties
        Properties properties = new Properties();

        //connect to Conduktor platform
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"4e6xurW4plU1y3AukyROPh\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI0ZTZ4dXJXNHBsVTF5M0F1a3lST1BoIiwib3JnYW5pemF0aW9uSWQiOjczMDMyLCJ1c2VySWQiOjg0ODk0LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIyZWYxYzE2Ny1lN2M3LTRmMmMtOTk3Yi04NWFhYzZiNzA0YjEifX0.rMdPlgEdilQ0v644CBpdJhp-vEfJVCauTrThlusBXd4\";");
        properties.setProperty("sasl.mechanism","PLAIN");

        //SET Consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                log.info("Detected a shutdown,let's by calling consumer.wakeup()..");
                consumer.wakeup();

                try{
                    mainThread.join();
                }
                catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        try
        {
            //subscribe to the topic
            consumer.subscribe(Arrays.asList(topic));
            //poll for data
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        }
        catch (WakeupException e){
            log.info("Consumer is starting to shutdown");
        }
        catch (Exception e){
            log.error("Unexpected Exception in the consumer",e);
        }
        finally {
            consumer.close();// close the consumer, this will also commit offsets
            log.info("consumer is now closing gracefully");
        }
    }
}
