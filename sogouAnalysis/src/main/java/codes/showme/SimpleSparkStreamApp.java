package codes.showme;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
//import org.apache.spark.streaming.kafka010.ConsumerStrategies;
//import org.apache.spark.streaming.kafka010.KafkaUtils;
//import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;
import java.util.regex.Pattern;


/**
 * Created by jack on 12/2/16.
 */
public class SimpleSparkStreamApp {
    public static String zkQuorum = "localhost:2181"; //is a list of one or more zookeeper servers that make quorum
    public static String group = "spark-consumer-group";   //<group> is the name of kafka consumer group
    public static String topic = "test";
    public static Integer numThreads = 2; // is the number of threads the kafka consumer should use
    private static final Pattern SPACE = Pattern.compile(" ");



    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setAppName("Spark Streaming")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("spark://192.168.7.152:7077");
        // create streaming context
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        // Checkpointing must be enabled to use the updateStateByKey function.
        //jssc.checkpoint("/tmp/log-analyzer-streaming");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "192.168.7.151:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // topic count map for kafka consumer
        Collection<String> topics = Arrays.asList("sogou", "topicB");

        final JavaInputDStream<ConsumerRecord<String, String>> stream =  KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                System.out.println(stringStringConsumerRecord.key());
                return stringStringConsumerRecord.value();
            }
        });

        // Split each line into words
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        words.print();




        // The processing can be manually stopped using jssc.stop();
        // just stop spark context jssc.stop(false);

        // Print the first ten elements of each RDD generated in this DStream to the console
//        wordCounts.print();
//        recentWordCounts.print();
        // Start the computation
        streamingContext.start();
        streamingContext.awaitTermination();

    }
}
