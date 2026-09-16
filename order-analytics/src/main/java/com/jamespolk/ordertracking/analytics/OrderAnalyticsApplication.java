package com.jamespolk.ordertracking.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class OrderAnalyticsApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderAnalyticsApplication.class, args);
    }
}
