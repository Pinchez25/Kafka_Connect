package com.pinchez.streams.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
class TopologyDescriptionLogger {

    private final StreamsBuilderFactoryBean streamsBuilder;

    @EventListener(ApplicationReadyEvent.class)
    void logTopology() {
        var topology = streamsBuilder.getTopology();
        if (topology != null) {
            log.info("Kafka Streams topology:\n{}", topology.describe());
        }
    }
}
