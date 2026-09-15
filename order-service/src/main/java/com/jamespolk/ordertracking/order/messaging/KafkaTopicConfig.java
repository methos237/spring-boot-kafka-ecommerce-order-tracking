package com.jamespolk.ordertracking.order.messaging;

import com.jamespolk.ordertracking.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
class KafkaTopicConfig {

    private static final int PARTITIONS = 3;

    @Bean
    NewTopic orderEvents() {
        return TopicBuilder.name(Topics.ORDER_EVENTS).partitions(PARTITIONS).build();
    }

    @Bean
    NewTopic paymentEvents() {
        return TopicBuilder.name(Topics.PAYMENT_EVENTS).partitions(PARTITIONS).build();
    }

    @Bean
    NewTopic inventoryEvents() {
        return TopicBuilder.name(Topics.INVENTORY_EVENTS).partitions(PARTITIONS).build();
    }

    /** One partition each: dead letters are rare and read by one operator-facing consumer. */
    @Bean
    NewTopic orderEventsDlt() {
        return TopicBuilder.name(Topics.ORDER_EVENTS + Topics.DLT_SUFFIX)
                .partitions(1)
                .build();
    }

    @Bean
    NewTopic paymentEventsDlt() {
        return TopicBuilder.name(Topics.PAYMENT_EVENTS + Topics.DLT_SUFFIX)
                .partitions(1)
                .build();
    }

    @Bean
    NewTopic inventoryEventsDlt() {
        return TopicBuilder.name(Topics.INVENTORY_EVENTS + Topics.DLT_SUFFIX)
                .partitions(1)
                .build();
    }
}
