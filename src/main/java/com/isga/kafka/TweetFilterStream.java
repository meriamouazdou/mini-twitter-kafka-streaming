package com.isga.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class TweetFilterStream {

    public static void main(String[] args) {

        // 1) Config Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweet-filter-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // 2) Topology
        StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream("tweets-input")
                .filter((k, v) -> v != null && v.contains("hashtags=kafka"))
                .to("tweets-kafka-filtered");

        Topology topology = builder.build();

        // 3) Start stream
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        System.out.println("✅ TweetFilterStream started...");

        // 4) Stop stream properly on exit
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
