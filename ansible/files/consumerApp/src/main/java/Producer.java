import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


/**
 * Created by jack on 4/3/16.
 */
public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {


        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:19092,localhost:19093");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        KafkaProducer producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 1000000; i++) {
            producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
        }

        producer.close();
    }

//    public static void createTopic() {
//        String zookeeperConnect = "zkserver1:2181,zkserver2:2181";
//        int sessionTimeoutMs = 10 * 1000;
//        int connectionTimeoutMs = 8 * 1000;
//        // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
//        // createTopic() will only seem to work (it will return without error).  The topic will exist in
//        // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
//        // topic.
//        ZkClient zkClient = new ZkClient(
//                zookeeperConnect,
//                sessionTimeoutMs,
//                connectionTimeoutMs,
//                ZKStringSerializer$.MODULE$);
//        zkClient.close();
//        // Security for Kafka was added in Kafka 0.9.0.0
//        boolean isSecureKafkaCluster = false;
//        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
//
//        String topic = "my-topic";
//        int partitions = 2;
//        int replication = 3;
//        Properties topicConfig = new Properties(); // add per-topic configurations settings here
//        AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig);
//
//
//
//    }
}
