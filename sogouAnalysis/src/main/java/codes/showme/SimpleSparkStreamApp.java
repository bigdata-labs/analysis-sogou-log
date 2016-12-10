package codes.showme;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiFunction;


/**
 * Created by jack on 12/2/16.
 */
public class SimpleSparkStreamApp {


    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setAppName("Spark Streaming")
                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("spark://192.168.7.152:7077");
        // create streaming context
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        // Checkpointing must be enabled to use the updateStateByKey function.
        streamingContext.checkpoint("/tmp/log-analyzer-streaming");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "192.168.7.151:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
//        kafkaParams.put("auto.offset.reset", "latest"); // for prod
        kafkaParams.put("auto.offset.reset", "earliest"); // for debug
        kafkaParams.put("enable.auto.commit", false);

        // topic count map for kafka consumer
        Collection<String> topics = Arrays.asList("sogou", "topicB");

        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                return stringStringConsumerRecord.value();
            }
        });

        // 00:00:26	9975666857142764	[电脑创业]	10 7	url
        JavaDStream<SearchRecord> searchRecords = lines.map(new Function<String, SearchRecord>() {
            @Override
            public SearchRecord call(String line) throws Exception {
                try {
                    if (StringUtils.isNotEmpty(line)) {
                        Map<String, Object> map = JSON.parseObject(line, new TypeReference<Map<String, Object>>() {
                        });
                        if (map != null && map.get("message") != null) {
                            String[] strSplit = ((String) map.get("message")).split("\\s");
                            if (strSplit.length == 6) {
                                SearchRecord searchRecord = new SearchRecord();
                                searchRecord.time(strSplit[0])
                                        .userId(strSplit[1])
                                        .queryWord(strSplit[2].substring(1, strSplit[2].length() - 1))
                                        .clickOrder(Integer.valueOf(strSplit[3]))
                                        .rankOfUrl(Integer.valueOf(strSplit[4]))
                                        .userClickUrl(strSplit[5]);
                                return searchRecord;
                            }
                        }
                    }
                } catch (Exception e) {
                    // todo
                    System.out.println(e);
                }
                return null;
            }
        });

        JavaPairDStream<String, String> userid_queryword = searchRecords.filter(Objects::nonNull).filter(r -> StringUtils.isNotEmpty(r.getQueryWord()))
                .mapToPair(searchRecord -> new Tuple2<>(searchRecord.getUserId(), searchRecord.getQueryWord()));

        JavaPairDStream<String, Iterable<String>> userid_querywordList = userid_queryword.groupByKey();


        JavaPairDStream<String, Map<String, Integer>> userid_querywordcountmap = userid_querywordList.filter(v1 -> v1._2() != null).mapValues(v1 -> {
            Map<String, Integer> word_count = new HashMap<>();
            for (String word : v1) {
                if (word_count.containsKey(word)) {
                    word_count.put(word, word_count.get(word) + 1);
                }else {
                    word_count.put(word, 1);
                }
            }
            return word_count;
        });

        userid_querywordcountmap.print();


        userid_querywordcountmap.foreachRDD(new VoidFunction2<JavaPairRDD<String, Map<String, Integer>>, Time>() {
            @Override
            public void call(JavaPairRDD<String, Map<String, Integer>> v1, Time v2) throws Exception {
                v1.foreachPartition(tuple2Iterator -> tuple2Iterator.forEachRemaining(stringMapTuple2 -> {
                    Jedis jedis = RedisConnection.getJedis();
                    jedis.sadd("userid", stringMapTuple2._1());
                    System.out.println("=========***********************");
                    System.out.println(stringMapTuple2._1() + " " + stringMapTuple2._2().keySet().size());
                    jedis.close();
                }));
            }
        });


//        userid_querywordcountmap.foreachRDD(new VoidFunction2<JavaPairRDD<String, Map<String, Integer>>, Time>() {
//            @Override
//            public void call(JavaPairRDD<String, Map<String, Integer>> v1, Time v2) throws Exception {
////                RedisConnection.jedis.zincrby()
//                RedisConnection.jedis.sadd("userid", v1.);
//                System.out.println("=========***********************");
//                System.out.println(v1.keys().first());
//            }
//        });

        // The processing can be manually stopped using jssc.stop();
        // just stop spark context jssc.stop(false);

        // Print the first ten elements of each RDD generated in this DStream to the console
        // Start the computation
        streamingContext.start();
        streamingContext.awaitTermination();


    }

    public static class RedisConnection implements Serializable {
        private static final long serialVersionUID = 5353378237055376883L;
//        public static final Jedis jedis = new Jedis("192.168.7.152");

        public static Jedis getJedis() {
            return new Jedis("192.168.7.152");
        }
    }
}

