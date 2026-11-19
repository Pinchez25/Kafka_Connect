package com.pinchez.streams.topology.fraud;

import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.FraudAlert;
import com.pinchez.events.FraudRule;
import com.pinchez.streams.config.StreamsProperties;
import com.pinchez.streams.mapper.FraudAlertMapper;
import com.pinchez.streams.topology.CdcSources;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

/**
 * Stream–table join + filter: a young account moving a large amount. The join already happened during enrichment, which
 * put the account's age on every transaction, so this rule is a stateless filter.
 */
public final class NewAccountLargeTransferRule implements FraudRuleTopology {

    private final StreamsProperties.Fraud.NewAccount settings;

    public NewAccountLargeTransferRule(StreamsProperties.Fraud.NewAccount settings) {
        this.settings = settings;
    }

    @Override
    public KStream<String, FraudAlert> detect(KStream<String, EnrichedTransaction> enriched, CdcSources sources) {
        return enriched.filter((_, transaction) -> matches(transaction), Named.as("new-account-large-transfer"))
                .mapValues(
                        (accountId, transaction) -> FraudAlertMapper.alert(
                                        FraudRule.NEW_ACCOUNT_LARGE_TRANSFER,
                                        accountId,
                                        transaction.getOccurredAt(),
                                        transaction.getTransactionId())
                                .setDescription("Account opened %s before sending %s (threshold %s)"
                                        .formatted(
                                                FraudAlertMapper.humanize(Duration.ofSeconds(transaction
                                                        .getAccountAgeSeconds()
                                                        .orElseThrow())),
                                                transaction.getAmount(),
                                                settings.amountThreshold()))
                                .setAmount(transaction.getAmount())
                                .setTransactionIds(List.of(transaction.getTransactionId()))
                                .build(),
                        Named.as("new-account-alert"));
    }

    private boolean matches(EnrichedTransaction transaction) {
        return FraudRuleTopology.isOutgoing(transaction)
                && transaction.getAmount().compareTo(settings.amountThreshold()) > 0
                && transaction
                        .getAccountAgeSeconds()
                        .filter(age -> age < settings.maxAge().toSeconds())
                        .isPresent();
    }
}
