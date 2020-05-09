package com.streming.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @fileName: KafkaQuickStart.java
 * @description: KafkaQuickStart.java类说明
 * @author: by echo huang
 * @date: 2020-03-31 16:31
 */
public class KafkaQuickStart {
    static String[] sandboxType = new String[]{"runtime", "simple"};

    private static String chooseSandBoxType() {
        return sandboxType[ThreadLocalRandom.current().nextInt(2)];
    }


    static String[] error_code = new String[]{"401", "502", "403", "500"};

    static Map<Integer, String> EVENT_TYPE = new HashMap<>();

    static {
        EVENT_TYPE.put(1, "RequestSend");
        EVENT_TYPE.put(2, "RequestFail1");
        EVENT_TYPE.put(0, "RequestSuccessTime");
    }

    private static String chooseCode() {
        return error_code[ThreadLocalRandom.current().nextInt(4)];
    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        AtomicInteger count = new AtomicInteger(1);
        AtomicInteger id = new AtomicInteger(1);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
                    String data = "{\n" +
                            "  \"distinct_id\": \"order\",\n" +
                            "  \"lib\": {\n" +
                            "    \"$lib\": \"js\",\n" +
                            "    \"$lib_method\": \"code\",\n" +
                            "    \"$lib_version\": \"1.14.16\"\n" +
                            "  },\n" +
                            "  \"properties\": {" +
                            "    \"$screen_height\": 768,\n" +
                            "    \"$screen_width\": 1366,\n" +
                            "    \"$lib\": \"js\",\n" +
                            "    \"$lib_version\": \"1.14.16\",\n" +
                            "    \"$latest_traffic_source_type\": \"直接流量\",\n" +
                            "    \"$latest_search_keyword\": \"未取到值_直接打开\",\n" +
                            "    \"$latest_referrer\": \"\",\n" +
                            "    \"tracking_scene\": \"沙盒性能监控\",\n" +
                            "    \"tracking_subscene\": \"练习\",  // 沙盒应用场景分为练习和课堂\n" +
                            "    \"api_name\": \"https://runtime-default.pandateacher.com/sandbox-python-025/default/0/32ce7ddc-1cd4-4e47-9654-99be2dddcb88-1-3/api/contents/practice/apps-1-id-5cd9765919bbcf00015547a4/root\",\n" +
                            "    \"api_type\": \"get\",\n" +
                            "    \"sandbox_type\": \"" + chooseSandBoxType() + "\",\n" +
                            "    \"request_action\": \"sandboxHttpRequestSend\",\n" +
                            "    \"request_time\": 56,\n" +
                            "    \"failure_type\": \"Error\",\n" +
                            "    \"error_reason\": \"Request failed with status code 404\",\n" +
                            "    \"error_code\": \"" + chooseCode() + "\",\n" +
                            "    \"net_type\": \"https\",\n" +
                            "    \"$is_first_day\": false,\n" +
                            "    \"$ip\": \"125.125.220.127\",  // 用户客户端 ip\n" +
                            "    \"$os\": \"Windows\",\n" +
                            "    \"$os_version\": \"7\",\n" +
                            "    \"$browser\": \"Sogou Explorer\",\n" +
                            "    \"$browser_version\": \"2.\",\n" +
                            "    \"$is_login_id\": true,\n" +
                            "    \"$city\": \"湖州\",\n" +
                            "    \"$province\": \"浙江\",\n" +
                            "    \"$country\": \"中国\"\n" +
                            "  },\n" +
                            "  \"login_id\": \"oRb8OwiHB1QInf57U2v6rWN3YzCM\",\n" +
                            "  \"anonymous_id\": \"1701f4f9b682a-0f018e50f9a3c7-5a4c2571-1049088-1701f4f9b6a58e\",\n" +
                            "  \"type\": \"track\",\n" +
                            "  \"event\": \"" + EVENT_TYPE.get(count.get() % 3) + "\", \n" +
                            "  \"_track_id\": 434851396,\n" +
                            "  \"time\": 1581483975323,\n" +
                            "  \"map_id\": \"oRb8OwiHB1QInf57U2v6rWN3YzCM\",\n" +
                            "  \"user_id\": -867276182798882600,\n" +
                            "  \"recv_time\": " + System.currentTimeMillis() + ", \n " +
                            "  \"extractor\": {\n" +
                            "    \"f\": \"(dev=fc01,ino=15214250)\",\n" +
                            "    \"o\": 139099797,\n" +
                            "    \"n\": \"access_log.2020021213\",\n" +
                            "    \"s\": 3147083838,\n" +
                            "    \"c\": 3147083838,\n" +
                            "    \"e\": \"data01.pandateacher.sa\"\n" +
                            "  },\n" +
                            "  \"project_id\": 2,\n" +
                            "  \"project\": \"production\",\n" +
                            "  \"ver\": 2\n" +
                            "}\n";
                    System.out.println(data);
                    count.incrementAndGet();
                    producer.send(new ProducerRecord<>("cep_test", data));
                }
                , 3, 5, TimeUnit.SECONDS
        );

    }
}
