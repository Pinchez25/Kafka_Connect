package com.pinchez.wallet.loadgen.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pinchez.TestcontainersConfiguration;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class LoadGeneratorIntegrationTest {

    @Autowired
    LoadGenerator loadGenerator;

    @Test
    void seedsDataAndGeneratesTrafficUntilStopped() {
        assertThat(loadGenerator.start().running()).isTrue();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var status = loadGenerator.status();
            assertThat(status.transactionsSubmitted().values().stream()
                            .mapToLong(Long::longValue)
                            .sum())
                    .isGreaterThanOrEqualTo(10);
            assertThat(status.errors()).isZero();
        });

        assertThat(loadGenerator.stop().running()).isFalse();
    }
}
