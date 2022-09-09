package com.example;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.MessageSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessagePipeline;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveMessagePipelineE2ETest {

    @Test
    void shouldConsumeMessages() throws Exception {
        try (PulsarClient pulsarClient = SingletonPulsarContainer.createPulsarClient()) {
            String topicName = "test" + UUID.randomUUID();
            // create subscription to retain messages
            pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName("sub").subscribe().close();

            ReactivePulsarClient reactivePulsarClient = AdaptedReactivePulsarClientFactory.create(pulsarClient);

            ReactiveMessageSender<String> messageSender = reactivePulsarClient
                .messageSender(Schema.STRING)
                .topic(topicName)
                .build();
            messageSender.sendMessages(Flux.range(1, 100).map(Object::toString).map(MessageSpec::of)).blockLast();

            List<String> messages = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch latch = new CountDownLatch(100);

            try (
                ReactiveMessagePipeline reactiveMessagePipeline = reactivePulsarClient
                    .messagePipeline(
                        reactivePulsarClient
                            .messageConsumer(Schema.STRING)
                            .subscriptionName("sub")
                            .topic(topicName)
                            .build()
                    )
                    .messageHandler(message ->
                        Mono.fromRunnable(() -> {
                            messages.add(message.getValue());
                            latch.countDown();
                        })
                    )
                    .build()
                    .start()
            ) {
                latch.await(5, TimeUnit.SECONDS);
                assertThat(messages).isEqualTo(Flux.range(1, 100).map(Object::toString).collectList().block());
            }
        }
    }
}
