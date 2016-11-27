import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by jack on 4/11/16.
 */
public class Consumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:19092,localhost:19093");
        props.put("group.id", "DemoConsumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "6000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer(props);


        kafkaConsumer.subscribe(Collections.singletonList("my-topic"));

        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(2000);
        System.out.println("------" + consumerRecords.count());

        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            System.out.println("===========================");
            System.out.println(consumerRecord.key() + " : " + consumerRecord.value() );
        }
        kafkaConsumer.commitSync();

        kafkaConsumer.close();



    }
}
