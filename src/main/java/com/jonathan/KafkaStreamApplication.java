package com.jonathan;

import com.jonathan.dto.ShopDTO;
import com.jonathan.streams.serializer.ShopSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class KafkaStreamApplication {
    private static final String SHOP_TOPIC = "SHOP_TOPIC";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "count-shops-by-users");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ShopSerde.class.getName());
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ShopDTO> inputTopic = builder.stream(SHOP_TOPIC);
        KTable<String, Long> comprasPorUsuario = inputTopic.groupByKey().count(Materialized.as("count-store"));
        comprasPorUsuario.toStream().print(Printed.toSysOut());
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

}
