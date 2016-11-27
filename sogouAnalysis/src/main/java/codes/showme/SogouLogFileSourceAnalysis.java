package codes.showme;

import com.google.common.collect.Lists;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Created by jack on 11/13/16.
 */
public class SogouLogFileSourceAnalysis implements Serializable {


    private static final Logger logger = LoggerFactory.getLogger(SogouLogFileSourceAnalysis.class);
    private static final long serialVersionUID = -7653619201089632357L;


    public static void main(String[] args) throws ParseException {

        new SogouLogFileSourceAnalysis().execute(getSparkMasterUrl(args), getAppName(args));

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

    public SogouLogFileSourceAnalysis(){

    }

    public void execute(String sparkMasterUrl, String appName){
//        Configuration configuration = new PropertiesConfig();
        SparkConf conf = new SparkConf().setAppName(appName)
                .setMaster(sparkMasterUrl);

        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> lines = sparkContext.textFile("file:///tmp/sogoulog.sample.log");
        System.out.println("lines count: " + lines.cache().count());


        JavaRDD<SearchRecord> rr = lines.map(new Function<String, SearchRecord>() {
            public SearchRecord call(String v1) throws Exception {
                String[] ss = v1.split("\\s");
                try {
                    return new SearchRecord()
                            .time(ss[0])
                            .userId(ss[1])
                            .queryWord(ss[2])
                            .rankOfUrl(Integer.valueOf(ss[3]))
                            .clickOrder(Integer.valueOf(ss[4]))
                            .userClickUrl(ss[5]);
                } catch (Exception e) {
                    System.out.println(v1);
                    return new SearchRecord();
                }

            }
        });

        rr.cache();

        JavaPairRDD<String, Iterable<SearchRecord>> jp = rr.groupBy(new Function<SearchRecord, String>() {
            public String call(SearchRecord v1) throws Exception {
                return v1.getUserId();
            }
        });

        // userId tags
        JavaPairRDD<String, Iterable<String>> userid_querywords = jp.mapValues(new Function<Iterable<SearchRecord>, Iterable<String>>() {
            public Iterable<String> call(final Iterable<SearchRecord> iterable) throws Exception {
                List<String> list = Lists.newArrayList();
                final Iterator<SearchRecord> iterator = iterable.iterator();
                while (iterator.hasNext()) {
                    final SearchRecord next = iterator.next();
                    if (next != null) {
                        list.add(next.getQueryWord());
                    }
                }
                return list;
            }
        });

        userid_querywords.saveAsTextFile("file:///tmp/userid_tags");

        sparkContext.stop();
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
