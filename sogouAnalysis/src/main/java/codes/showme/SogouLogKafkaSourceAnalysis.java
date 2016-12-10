package codes.showme;

import codes.showme.config.PropertiesConfig;
import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ProcessedOffsetManager;
import consumer.kafka.ReceiverLauncher;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;



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
        props.put("consumer.forcefromstart", "true");
        System.out.println("zookeeper: " + props.getProperty("zookeeper.hosts"));
        System.out.println("kafka.topic: " + props.getProperty("kafka.topic"));

        SparkConf _sparkConf = new SparkConf();
        if (StringUtils.isNotBlank(sparkMasterUrl)) {
            _sparkConf.setMaster(sparkMasterUrl);
        }

        if (StringUtils.isNotBlank(appName)) {
            _sparkConf.setAppName(appName);
        }

        JavaStreamingContext jsc = new JavaStreamingContext(_sparkConf, Durations.seconds(2));
        // Specify number of Receivers you need.
        int numberOfReceivers = 2;
        System.out.println("=======================1");

        JavaDStream<MessageAndMetadata> unionStreams = ReceiverLauncher.launch(
                jsc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY());
        System.out.println("=======================2");

        //Get the Max offset from each RDD Partitions. Each RDD Partition belongs to One Kafka Partition
        JavaPairDStream<Integer, Iterable<Long>> partitonOffset = ProcessedOffsetManager
                .getPartitionOffset(unionStreams);

        System.out.println("=================    3");

        //Start Application Logic
//        unionStreams.foreachRDD(new Function<JavaRDD<MessageAndMetadata>, Void>() {
//            @Override
//            public Void call(JavaRDD<MessageAndMetadata> v1) throws Exception {
//
//
//            }
//        });


        System.out.println("=================    4");

        //End Application Logic

        //Persists the Max Offset of given Kafka Partition to ZK
        ProcessedOffsetManager.persists(partitonOffset, props);
        jsc.start();
        jsc.awaitTermination();
        System.out.println("3------------------------------------------");

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
