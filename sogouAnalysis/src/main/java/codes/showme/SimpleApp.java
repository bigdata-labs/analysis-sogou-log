package codes.showme;

import codes.showme.config.PropertiesConfig;
import com.google.common.collect.Lists;
import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ProcessedOffsetManager;
import consumer.kafka.ReceiverLauncher;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;

/**
 *
 */
public class SimpleApp implements Serializable {


    private static final Logger logger = LoggerFactory.getLogger(SimpleApp.class);

    private static final long serialVersionUID = 651154131299369784L;


    public static void main(String[] args) throws ParseException, InterruptedException, IOException {
        new SimpleApp().execute(getSparkMasterUrl(args), getAppName(args));
    }


    public SimpleApp() {

    }

    public void execute(String sparkMasterUrl, String appName) throws InterruptedException, IOException {


        Properties props = new PropertiesConfig().getProperties();

        SparkConf _sparkConf = new SparkConf();
        if (StringUtils.isNotBlank(sparkMasterUrl)) {
            _sparkConf.setMaster(sparkMasterUrl);
        }

        if (StringUtils.isNotBlank(appName)) {
            _sparkConf.setAppName(appName);
        }

        logger.info(appName);
        logger.info(sparkMasterUrl);

        System.out.println("----------------");
        logger.info("======================");


        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load our input data.
        JavaRDD<String> input = sc.textFile((String)props.getOrDefault("wordcount_file_path", "file:///home/spark/spark/wordcount.txt"));

        // Split up into words.
        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        // Transform into word and count.
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                x -> new Tuple2(x, 1)).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        });
        // Save the word count back out to a text file, causing evaluation.
        counts.collect().forEach(stringIntegerTuple2 -> System.out.println(stringIntegerTuple2._1() + " " + stringIntegerTuple2._2()));
        counts.saveAsTextFile("file:///tmp/spark-" + Instant.now().toString());
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


}
