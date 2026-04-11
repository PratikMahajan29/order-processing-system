package com.ops.order._processing.config;

import com.ops.order._processing.event.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;


import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, OrderEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.springframework.kafka.support.serializer.JsonDeserializer");
        props.put("spring.json.trusted.packages", "com.ops.order._processing.event");
        props.put("spring.json.value.default.type", "com.ops.order._processing.event.OrderEvent");

        return new org.springframework.kafka.core.DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, OrderEvent> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(6);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, OrderEvent> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, ex) -> new TopicPartition("order-topic-dlq", record.partition())
                );

        recoverer.setHeadersFunction((record, ex) -> {
            org.apache.kafka.common.header.internals.RecordHeaders headers =
                    new org.apache.kafka.common.header.internals.RecordHeaders();

            Throwable root = ex.getCause() != null ? ex.getCause() : ex;

            headers.add("exception-type", root.getClass().getSimpleName().getBytes());
            headers.add("error-message", root.getMessage().getBytes());
            return headers;
        });

        FixedBackOff backOff = new FixedBackOff(0L, 0L); // No retries, immediately send to DLQ

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // This tells the error handler to not retry when NonRetryableException is thrown and directly send the message to DLQ.
        errorHandler.addNotRetryableExceptions(com.ops.order._processing.exception.NonRetryableException.class);

        return errorHandler;
    }
}
