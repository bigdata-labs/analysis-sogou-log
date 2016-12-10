package codes.showme;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import redis.clients.jedis.Jedis;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Created by jack on 12/10/16.
 */
public class FilebeatMessage implements Serializable {

    private static final long serialVersionUID = -1568293311604326053L;

    public static void main(String[] args) {
        String json = "{\"@timestamp\":\"2016-12-10T09:49:28.639Z\",\n" +
                "            \"beat\":{\"hostname\":\"nodelab2\", \"name\":\"nodelab2\",\"version\":\"5.0.0\"},\n" +
                "        \"input_type\":\"log\",\n" +
                "            \"message\":\"00:09:40\\t5668233219730905\\t[普利司通轮胎报价]\\t1 6\\tbbs.speedyou.com/cgi-bin/topic.cgi?forum=107\\u0026topic=4\",\"offset\":948048,\"source\":\"/var/log/sogou/s2.log\",\"type\":\"log\"}";

        Map<String, Object> map = JSON.parseObject(json, new TypeReference<Map<String, Object>>() {
        });
        Stream.of(((String) map.get("message")).split("\\s")).forEach(System.out::println);
        System.out.println();


        Jedis jedis = new Jedis("192.168.7.152");
        jedis.set("xxx", "xxx");
        jedis.close();
    }


}
