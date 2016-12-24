package codes.showme;

import codes.showme.config.Configuration;
import codes.showme.config.PropertiesConfig;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
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
import redis.clients.jedis.Transaction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;


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

        // topic count map for kafka consumer
        Collection<String> topics = Arrays.asList("sogou");

        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        JavaDStream<String> lines = stream.map(ConsumerRecord::value);

        JavaDStream<SearchRecord> searchRecords = lines.map(line -> {
            try {
                if (StringUtils.isNotEmpty(line)) {
                    Map<String, Object> map = JSON.parseObject(line, new TypeReference<Map<String, Object>>() {
                    });
                    if (map != null && map.get("message") != null) {
                        return convertFromLine(((String) map.get("message")));
                    }
                }
            } catch (Exception e) {
                logger.error("", e);
                return null;
            }
            return null;
        });


//        searchRecords.cache();


        // 热词榜
        JavaPairDStream<String, Integer> word_count = searchRecords
                .filter(Objects::nonNull)
                .filter(r -> StringUtils.isNotEmpty(r.getQueryWord()))
                .map(SearchRecord::getQueryWord)
                .mapToPair(s -> Tuple2.apply(s, 1))
                .reduceByKey((v1, v2) -> v1 + v2);

        word_count.foreachRDD(wcRDD -> {
            if (!wcRDD.isEmpty()) {
                wcRDD.foreachPartition(tuple2Iterator -> {
                    int redisDb = 2;
                    Jedis jedis = RedisConnection.getJedis();
                    jedis.select(redisDb);
                    Transaction transaction = jedis.multi();
                    tuple2Iterator.forEachRemaining(stringIntegerTuple2 -> {
                        transaction.zincrby("hotwordlist", stringIntegerTuple2._2(), stringIntegerTuple2._1());
                    });
                    transaction.exec();
                    jedis.close();
                });
            }

        });


        // 对于词,每个用户的热度
        JavaPairDStream<String, Map<String, Integer>> querywordcount_userid_map = convertToWordUserIdCountMap(searchRecords);
        querywordcount_userid_map.foreachRDD(stringMapJavaPairRDD -> {
            stringMapJavaPairRDD.foreachPartition(tuple2Iterator -> {
                int redisDb = 1;
                Jedis jedis = RedisConnection.getJedis();
                jedis.select(redisDb);
                Transaction transaction = jedis.multi();
                tuple2Iterator.forEachRemaining(stringMapTuple2 -> {
                    for (Map.Entry<String, Integer> stringIntegerEntry : stringMapTuple2._2().entrySet()) {
                        transaction.zincrby(stringMapTuple2._1(), stringIntegerEntry.getValue(), stringIntegerEntry.getKey());
                    }
                });
                transaction.exec();
                jedis.close();
            });

        });

        // 对于用户,词的热度
        JavaPairDStream<String, Map<String, Integer>> userid_querywordcountmap = convertToUseridQuerywordcountmap(searchRecords);
        userid_querywordcountmap.foreachRDD((v1, v2) -> {
            v1.foreachPartition(tuple2Iterator -> {
                int redisDb = 0;
                Jedis jedis = RedisConnection.getJedis();
                jedis.select(redisDb);
                Transaction transaction = jedis.multi();
                tuple2Iterator.forEachRemaining(stringMapTuple2 -> {
                    for (Map.Entry<String, Integer> stringIntegerEntry : stringMapTuple2._2().entrySet()) {
                        transaction.zincrby(stringMapTuple2._1(), stringIntegerEntry.getValue(), stringIntegerEntry.getKey());
                    }
                });
                transaction.exec();
                jedis.close();
            });
        });

        streamingContext.start();
        streamingContext.awaitTermination();


    }

    private static void saveStringMapTuple2ToRedis(Tuple2<String, Map<String, Integer>> stringMapTuple2, int redisDb) {
        Jedis jedis = RedisConnection.getJedis();
        jedis.select(redisDb);
        Transaction transaction = jedis.multi();
        for (Map.Entry<String, Integer> stringIntegerEntry : stringMapTuple2._2().entrySet()) {
            transaction.zincrby(stringMapTuple2._1(), stringIntegerEntry.getValue(), stringIntegerEntry.getKey());
        }
        transaction.exec();
        jedis.close();
    }

    public static class RedisConnection implements Serializable {
        private static final long serialVersionUID = 5353378237055376883L;

        public static Jedis getJedis() {
            return new Jedis(configuration.getRedisHost());
        }
    }

    private static JavaPairDStream<String, Map<String, Integer>> convertToUseridQuerywordcountmap(JavaDStream<SearchRecord> searchRecords) {
        JavaPairDStream<String, String> userid_queryword = searchRecords
                .filter(Objects::nonNull)
                .filter(r -> StringUtils.isNotEmpty(r.getQueryWord()))
                .mapToPair(searchRecord -> new Tuple2<>(searchRecord.getUserId(), searchRecord.getQueryWord()));

        JavaPairDStream<String, Iterable<String>> userid_querywordList = userid_queryword.groupByKey();


        JavaPairDStream<String, Map<String, Integer>> userid_querywordcountmap = userid_querywordList.filter(v1 -> v1._2() != null).mapValues(new Function<Iterable<String>, Map<String, Integer>>() {
            @Override
            public Map<String, Integer> call(Iterable<String> v1) throws Exception {
                Map<String, Integer> word_count = new HashMap<>();
                for (String word : v1) {
                    if (word_count.containsKey(word)) {
                        word_count.put(word, word_count.get(word) + 1);
                    } else {
                        word_count.put(word, 1);
                    }
                }
                return word_count;
            }
        });

        return userid_querywordcountmap;

    }


    private static JavaPairDStream<String, Map<String, Integer>> convertToWordUserIdCountMap(JavaDStream<SearchRecord> searchRecords) {
        JavaPairDStream<String, String> word_userid = searchRecords
                .filter(Objects::nonNull)
                .filter(r -> StringUtils.isNotEmpty(r.getQueryWord()))
                .mapToPair(searchRecord -> new Tuple2<>(searchRecord.getUserId(), searchRecord.getWordsAfterSegment()))
                .flatMapValues(v1 -> v1)
                .mapToPair(v -> Tuple2.apply(v._2(), v._1()));

        JavaPairDStream<String, Iterable<String>> queryword_userid_List = word_userid.groupByKey();
        return queryword_userid_List.filter(v1 -> v1._2() != null).mapValues(v1 -> {
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


    }

    private static SearchRecord convertFromLine(String line) {
        String[] strSplit = line.split("\\s");
        if (strSplit.length == 6) {
            SearchRecord searchRecord = new SearchRecord();
            searchRecord.time(strSplit[0])
                    .userId(strSplit[1])
                    .queryWord(strSplit[2].substring(1, strSplit[2].length() - 1))
                    .clickOrder(Integer.valueOf(strSplit[3]))
                    .rankOfUrl(Integer.valueOf(strSplit[4]))
                    .userClickUrl(strSplit[5]);
            return searchRecord;
        } else {
            return null;
        }
    }
}

