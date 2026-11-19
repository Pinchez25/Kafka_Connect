package com.pinchez.streams.extractor;

import com.pinchez.cdc.Account;
import com.pinchez.cdc.Merchant;
import com.pinchez.cdc.Transaction;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Event time from the row itself rather than the Kafka record timestamp, which only says when Connect produced the
 * record. Tombstones carry no row, so they fall back to the record timestamp.
 */
public final class CdcTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        return switch (record.value()) {
            case Transaction transaction -> toMillis(transaction.getCreatedAt());
            case Account account -> toMillis(account.getUpdatedAt());
            case Merchant merchant -> toMillis(merchant.getCreatedAt());
            case null, default -> record.timestamp() >= 0 ? record.timestamp() : partitionTime;
        };
    }

    private static long toMillis(String isoTimestamp) {
        return Instant.parse(isoTimestamp).toEpochMilli();
    }
}
