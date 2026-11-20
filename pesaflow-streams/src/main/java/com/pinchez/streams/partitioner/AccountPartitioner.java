package com.pinchez.streams.partitioner;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * Places a record by its account id exactly as Kafka's default partitioner places a string key, because the topics it
 * writes are joined with {@code pesaflow.accounts}, which Connect partitions that way. Any other rule here would break
 * co-partitioning (and interactive-query routing) without an error.
 */
public final class AccountPartitioner<V> implements StreamPartitioner<String, V> {

    @Override
    public Optional<Set<Integer>> partitions(String topic, String accountId, V value, int numPartitions) {
        if (accountId == null) {
            return Optional.empty();
        }
        var hash = Utils.murmur2(accountId.getBytes(StandardCharsets.UTF_8));
        return Optional.of(Set.of(Utils.toPositive(hash) % numPartitions));
    }
}
