package com.isga.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;


public class TweetCountStream {

    public static void main(String[] args) {

        // 1️⃣ Configuration de Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweet-count-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // 2️⃣ Construction du flux
        StreamsBuilder builder = new StreamsBuilder();

        // Lecture du topic tweets-input
        KStream<String, String> tweets = builder.<String, String>stream("tweets-input");

        // 3️⃣ Extraction du user_id depuis chaque message
        KStream<String, String> tweetsByUser = tweets
                .filter((k, v) -> v != null && v.contains("user_id="))
                .map((k, v) -> {
                    String userId = extractUserId(v);
                    return KeyValue.pair(userId, v);
                });

        // 4️⃣ Groupement par user_id et comptage
        KTable<String, Long> tweetCounts = tweetsByUser
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("tweet-counts-store"));

        // 5️⃣ Écriture du résultat dans le topic tweet-counts-output
        tweetCounts
                .toStream()
                .mapValues((key, value) -> value.toString())
                .to("tweet-counts-output", Produced.with(Serdes.String(), Serdes.String()));

        // 6️⃣ Construction et lancement
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        System.out.println("✅ TweetCountStream started... Counting tweets per user...");

        // 7️⃣ Fermeture propre à l’arrêt
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    // --------------------------------------------------
    // Méthode utilitaire : extraire user_id du message
    // --------------------------------------------------
    private static String extractUserId(String message) {
        try {
            // message = {"message":"user_id=u3; timestamp=..."}
            int start = message.indexOf("user_id=") + 8;
            int end = message.indexOf(";", start);
            if (end == -1) end = message.length();
            return message.substring(start, end).trim();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
