package com.pinchez.streams.partitioner;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.junit.jupiter.api.Test;

class AccountPartitionerTest {

    private final AccountPartitioner<Object> partitioner = new AccountPartitioner<>();

    @Test
    void agreesWithThePartitionerConnectUsesForPesaflowAccounts() {
        IntStream.range(0, 1_000).mapToObj(_ -> UUID.randomUUID().toString()).forEach(accountId -> {
            var kafkaDefault = BuiltInPartitioner.partitionForKey(accountId.getBytes(StandardCharsets.UTF_8), 3);
            assertThat(partitioner.partitions("any", accountId, null, 3)).contains(Set.of(kafkaDefault));
        });
    }

    @Test
    void leavesKeylessRecordsToKafka() {
        assertThat(partitioner.partitions("any", null, null, 3)).isEmpty();
    }
}
