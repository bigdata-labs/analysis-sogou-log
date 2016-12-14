package codes.showme;

import codes.showme.config.Configuration;
import codes.showme.config.PropertiesConfig;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;


/**
 * Created by jack on 12/2/16.
 */
public class SogouPersona {
    private static final Logger logger = LoggerFactory.getLogger(SogouPersona.class);

    private static Configuration configuration = new PropertiesConfig();

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setAppName(configuration.getAppName())
                .setMaster(configuration.getSparkMasterURL());

        // create streaming context
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        // Checkpointing must be enabled to use the updateStateByKey function.
        streamingContext.checkpoint(configuration.getSparkCheckpoint());

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", configuration.getKafkaBootstrapServers());
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
//        kafkaParams.put("auto.offset.reset", "latest"); // for prod
        kafkaParams.put("auto.offset.reset", configuration.getKafkaAutoOffsetReset()); // for debug
        kafkaParams.put("enable.auto.commit", configuration.getKafkaEnableAutoCommit());

        System.out.println("--------------------");
        System.out.println(configuration.getRedisHost());

        // topic count map for kafka consumer
        Collection<String> topics = Arrays.asList("sogou");

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

        JavaDStream<SearchRecord> searchRecords = lines.map(new Function<String, SearchRecord>() {
            @Override
            public SearchRecord call(String line) throws Exception {
                try {
                    if (StringUtils.isNotEmpty(line)) {
                        System.out.println("-**************");
                        System.out.println(line);
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
                    logger.error("", e);
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
                } else {
                    word_count.put(word, 1);
                }
            }
            return word_count;
        });

        userid_querywordcountmap.foreachRDD(new VoidFunction2<JavaPairRDD<String, Map<String, Integer>>, Time>() {
            @Override
            public void call(JavaPairRDD<String, Map<String, Integer>> v1, Time v2) throws Exception {
                v1.foreachPartition(tuple2Iterator -> {
                    tuple2Iterator.forEachRemaining(stringMapTuple2 -> {
                        Jedis jedis = RedisConnection.getJedis();
                        Pipeline pipeline = jedis.pipelined();
                        System.out.println("pipeline" + pipeline);
                        for (Map.Entry<String, Integer> stringIntegerEntry : stringMapTuple2._2().entrySet()) {
                            pipeline.zincrby(stringMapTuple2._1(), stringIntegerEntry.getValue(), stringIntegerEntry.getKey());
                            System.out.println(stringIntegerEntry.getValue());
                        }
                        pipeline.sync();
                        jedis.close();
                    });

                });
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();


    }

    public static class RedisConnection implements Serializable {
        private static final long serialVersionUID = 5353378237055376883L;
        public static Jedis getJedis() {
            return new Jedis(configuration.getRedisHost());
        }
    }
}

