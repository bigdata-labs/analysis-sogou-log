package codes.showme;

import codes.showme.config.PropertiesConfig;
import com.google.common.collect.Lists;
import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ProcessedOffsetManager;
import consumer.kafka.ReceiverLauncher;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 *
 */
public class SogouLogKafkaSourceAnalysis implements Serializable {


    private static final Logger logger = LoggerFactory.getLogger(SogouLogKafkaSourceAnalysis.class);

    private static final long serialVersionUID = 651154131299369784L;


    public static void main(String[] args) throws ParseException, InterruptedException, IOException {
        new SogouLogKafkaSourceAnalysis().execute("spark://192.168.7.152:7077", "kafka");
    }


    public SogouLogKafkaSourceAnalysis() {

    }

    public void execute(String sparkMasterUrl, String appName) throws InterruptedException, IOException {
        System.out.println("-------------------------------------------------");

        System.out.println(sparkMasterUrl);

        System.out.println(appName);

        Properties props = new PropertiesConfig().getProperties();
        System.out.println("zookeeper: " + props.getProperty("zookeeper.hosts"));
        System.out.println("kafka.topic: " + props.getProperty("kafka.topic"));

        SparkConf _sparkConf = new SparkConf();
        if (StringUtils.isNotBlank(sparkMasterUrl)) {
            _sparkConf.setMaster(sparkMasterUrl);
        }

        if (StringUtils.isNotBlank(appName)) {
            _sparkConf.setAppName(appName);
        }
        _sparkConf.set("spark.cores.max", "1");

        JavaStreamingContext jsc = new JavaStreamingContext(_sparkConf, Durations.seconds(1));
        // Specify number of Receivers you need.
        int numberOfReceivers = 1;
        System.out.println("=======================1");

        JavaDStream<MessageAndMetadata> unionStreams = ReceiverLauncher.launch(
                jsc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY());
        System.out.println("=======================2");

        //Get the Max offset from each RDD Partitions. Each RDD Partition belongs to One Kafka Partition
        JavaPairDStream<Integer, Iterable<Long>> partitonOffset = ProcessedOffsetManager
                .getPartitionOffset(unionStreams);

        System.out.println("=================    3");

        //Start Application Logic
        unionStreams.foreachRDD((rdd, v2) -> {
            List<MessageAndMetadata> rddList = rdd.collect();
            System.out.println(" Number of records in this batch " + rddList.size());

            SearchRecord searchRecord = new SearchRecord();
            List<String> lines = Lists.newArrayList();
            for (MessageAndMetadata messageAndMetadata : rddList) {
                lines.add(messageAndMetadata.getConsumer());
                System.out.println("0000000000000000000000000000000000000000000000000000000000000000000000000000");
                System.out.println(messageAndMetadata.getConsumer() + " -----  " + messageAndMetadata.getKey());

            }

            return;
        });
        System.out.println("=================    4");

        //End Application Logic

        //Persists the Max Offset of given Kafka Partition to ZK
        ProcessedOffsetManager.persists(partitonOffset, props);
        jsc.start();
        jsc.awaitTermination();
        System.out.println("3------------------------------------------");

    }

    /**
     * Created by jack on 11/13/16.
     */
    public static class SearchRecord implements Serializable {


        private static final long serialVersionUID = -9152786748871216711L;
        private String userId;
        private String time;
        private String queryWord;
        private String userClickUrl;
        private int rankOfUrl;
        private int clickOrder;

        public SearchRecord userId(String userId) {
            this.userId = userId;
            return this;
        }

        public SearchRecord time(String time) {
            this.time = time;
            return this;
        }

        public SearchRecord queryWord(String queryWord) {
            this.queryWord = queryWord;
            return this;
        }

        public SearchRecord userClickUrl(String userClickUrl) {
            this.userClickUrl = userClickUrl;
            return this;
        }

        public SearchRecord rankOfUrl(int rankOfUrl) {
            this.rankOfUrl = rankOfUrl;
            return this;
        }

        public SearchRecord clickOrder(int clickOrder) {
            this.clickOrder = clickOrder;
            return this;
        }

        public String getUserId() {
            return userId;
        }

        public String getTime() {
            return time;
        }

        public String getQueryWord() {
            return queryWord;
        }

        public String getUserClickUrl() {
            return userClickUrl;
        }

        public int getRankOfUrl() {
            return rankOfUrl;
        }

        public int getClickOrder() {
            return clickOrder;
        }


        @Override
        public String toString() {
            return "SearchRecord{" +
                    "userId='" + userId + '\'' +
                    ", time='" + time + '\'' +
                    ", queryWord='" + queryWord + '\'' +
                    ", userClickUrl='" + userClickUrl + '\'' +
                    ", rankOfUrl=" + rankOfUrl +
                    ", clickOrder=" + clickOrder +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SearchRecord that = (SearchRecord) o;

            if (rankOfUrl != that.rankOfUrl) return false;
            if (clickOrder != that.clickOrder) return false;
            if (userId != null ? !userId.equals(that.userId) : that.userId != null) return false;
            if (time != null ? !time.equals(that.time) : that.time != null) return false;
            if (queryWord != null ? !queryWord.equals(that.queryWord) : that.queryWord != null) return false;
            return !(userClickUrl != null ? !userClickUrl.equals(that.userClickUrl) : that.userClickUrl != null);

        }

        @Override
        public int hashCode() {
            int result = userId != null ? userId.hashCode() : 0;
            result = 31 * result + (time != null ? time.hashCode() : 0);
            result = 31 * result + (queryWord != null ? queryWord.hashCode() : 0);
            result = 31 * result + (userClickUrl != null ? userClickUrl.hashCode() : 0);
            result = 31 * result + rankOfUrl;
            result = 31 * result + clickOrder;
            return result;
        }
    }

    public static String getSparkMasterUrl(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("master", true, "the master url of spark");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd.getOptionValue("master", "local[2]");
    }

    public static String getAppName(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("name", true, "the app name of this program in spark");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd.getOptionValue("name", "appname" + new Random(1000).nextLong());
    }

    public static String getKafkaServers(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("kafkaServers", true, "kafka params of bootstrap.servers");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd.getOptionValue("kafkaServers");
    }

}
