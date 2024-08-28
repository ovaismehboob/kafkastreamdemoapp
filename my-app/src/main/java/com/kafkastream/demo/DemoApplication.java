package com.kafkastream.demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
//import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        // Step 1: Create a Properties object
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "demo-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "XXXXXXXXXXXXXXXXXX.servicebus.windows.net:9093");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        String jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\""
            + " password=\"Endpoint=sb://XXXXXXXXXXXXXXXXXX.servicebus.windows.net/;SharedAccessKeyName="
            + "XXXXXXXXXXXXXXXXXX;SharedAccessKey="
            + "XXXXXXXXXXXXXXXXXX" + "\";";

        properties.put("sasl.jaas.config", jaasConfig);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Step 2: Build the topology
        Topology topology = buildTopology("input-topic", "output-topic");

        // Step 3: Create a KafkaStreams instance
        KafkaStreams streams = new KafkaStreams(topology, properties);

		System.out.println("**************************************************");
		System.out.println(("Starting Kafka Streams"));
		System.out.println("**************************************************");
		        // Step 4: Start the Kafka Streams application
        streams.start();

        // Add shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


	public static Topology buildTopology(String inputTopic, String outputTopic) {
		
           
        Serde<String> stringSerde = Serdes.String();
		StreamsBuilder builder = new StreamsBuilder();

        // KTable 

        KTable<String, String> table = builder.table(inputTopic, Consumed.with(stringSerde, stringSerde), Materialized.as("ktable-store"));

        table
            .toStream()
            .peek((k, v) -> System.out.println("Observed event: " + v))
            .mapValues(s -> s.toUpperCase())
            .peek((k, v) -> System.out.println("Observed event: " + v))
            .to(outputTopic, Produced.with(stringSerde, stringSerde));

        
        // Kafka Stream 
        
	//	builder
	//		.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
	//		.peek((k, v) -> System.out.println("Observed event: " + v))
	//		.mapValues(s -> s.toUpperCase())
	//		.peek((k, v) -> System.out.println("Observed event: " + v))
	//		.to(outputTopic, Produced.with(stringSerde, stringSerde));

		return builder.build();
	}

}
