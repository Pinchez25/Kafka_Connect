package com.pinchez.streams.topology.fraud;

import com.pinchez.events.FraudAlert;
import com.pinchez.streams.support.TopologyHarness;
import java.time.Instant;
import java.time.LocalDateTime;
import org.apache.kafka.streams.TestOutputTopic;

final class FraudRuleTestSupport {

    static final Instant NINE_AM_UTC = Instant.parse("2026-11-18T09:00:00Z");

    private FraudRuleTestSupport() {}

    static TestOutputTopic<String, FraudAlert> alerts(TopologyHarness harness) {
        return harness.output(TopologyHarness.TOPICS.fraudAlerts());
    }

    static Instant nairobi(String localDateTime) {
        return LocalDateTime.parse(localDateTime)
                .atZone(TopologyHarness.NAIROBI)
                .toInstant();
    }
}
