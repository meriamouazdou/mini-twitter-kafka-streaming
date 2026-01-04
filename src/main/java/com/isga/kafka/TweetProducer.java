/*package com.isga.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TweetProducer {

    public static void main(String[] args) {

        // Configuration de base Kafka
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Création du producteur
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Message exemple
        String message = "{\"message\":\"user_id=u1; timestamp=2025-05-22T10:00:00; tweet_text=Hello world!; hashtags=java,kafka\"}";

        // Envoi dans tweets-input
        ProducerRecord<String, String> record =
                new ProducerRecord<>("tweets-input", null, message);

        producer.send(record);
        producer.close();

        System.out.println(" Bravooooo merii 👏🏻👏🏻👏🏻Message envoyé !");
    }
}*/
package com.isga.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class TweetProducer {

    public static void main(String[] args) {

        // 1) Properties Kafka Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();
        String[] hashtagsList = {"java", "kafka", "bigdata", "ai", "cloud", "ml"};

        for (int i = 0; i < 100; i++) {

            String user_id = "u" + (random.nextInt(10) + 1);   // u1..u10
            String timestamp = Instant.now().toString();
            String tweetText = "Tweet numéro " + i;

            // pick 1–3 random hashtags
            int count = random.nextInt(3) + 1;
            StringBuilder hashtags = new StringBuilder();
            for (int j = 0; j < count; j++) {
                hashtags.append(hashtagsList[random.nextInt(hashtagsList.length)]);
                if (j < count - 1) hashtags.append(",");
            }

            // internal message
            String innerMsg = "user_id=" + user_id +
                    "; timestamp=" + timestamp +
                    "; tweet_text=" + tweetText +
                    "; hashtags=" + hashtags;

            // JSON wrapper
            String json = "{\"message\":\"" + innerMsg + "\"}";

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("tweets-input", null, json);

            producer.send(record);

            System.out.println("✅ Sent → " + json);
        }

        producer.close();
        System.out.println("✅✅ 100 messages envoyés !");
    }
}

