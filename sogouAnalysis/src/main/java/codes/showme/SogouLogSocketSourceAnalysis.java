package codes.showme;

import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

/**
 * 启动后,mac环境使用nc -lp 9999 命令向spark stream 写数据
 */
public class SogouLogSocketSourceAnalysis implements Serializable {


    private static final Logger logger = LoggerFactory.getLogger(SogouLogSocketSourceAnalysis.class);
    private static final long serialVersionUID = 651154131299369784L;


    public static void main(String[] args) throws ParseException, InterruptedException {
        new SogouLogSocketSourceAnalysis().execute(getSparkMasterUrl(args), getAppName(args));
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

    public SogouLogSocketSourceAnalysis() {

    }

    public void execute(String sparkMasterUrl, String appName) throws InterruptedException {
//        Configuration configuration = new PropertiesConfig();
        SparkConf conf = new SparkConf().setAppName(appName)
                .setMaster(sparkMasterUrl);

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));


        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String x) {
                        return Arrays.asList(x.split(" ")).iterator();
                    }
                });

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate

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
}
