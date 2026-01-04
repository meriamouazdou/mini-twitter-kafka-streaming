package com.isga.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TweetConsumer {

    public static void main(String[] args) {

        // 1) Config Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "tweet-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList("tweets-input"));

            System.out.println("✅ TweetConsumer started...");

            while (true) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {

                    String json = record.value();

                    // Extract inner "message"
                    String inner = extractMessage(json);

                    // Parse key=value
                    parseAndPrint(inner);
                }
            }
        }
    }

    // ----------------------------
    // extract "message": "...inner..."
    // ----------------------------
    private static String extractMessage(String json) {
        // find "message":"   then take until last quote
        int start = json.indexOf("\"message\":\"") + 10;
        int end = json.lastIndexOf("\"");
        return json.substring(start, end);
    }

    // ----------------------------
    // Split "user_id=...; timestamp=...; tweet_text=...; hashtags=..."
    // ----------------------------
    private static void parseAndPrint(String inner) {

        String[] parts = inner.split(";");

        String user_id = "";
        String timestamp = "";
        String tweet_text = "";
        String hashtags = "";

        for (String part : parts) {
            String[] kv = part.trim().split("=", 2);
            if (kv.length < 2) continue;
            String key = kv[0].trim();
            String value = kv[1].trim();

            switch (key) {
                case "user_id":     user_id = value; break;
                case "timestamp":   timestamp = value; break;
                case "tweet_text":  tweet_text = value; break;
                case "hashtags":    hashtags = value; break;
            }
        }

        System.out.println("-----------");
        System.out.println("user_id    : " + user_id);
        System.out.println("timestamp  : " + timestamp);
        System.out.println("tweet_text : " + tweet_text);
        System.out.println("hashtags   : " + hashtags);
    }
}
