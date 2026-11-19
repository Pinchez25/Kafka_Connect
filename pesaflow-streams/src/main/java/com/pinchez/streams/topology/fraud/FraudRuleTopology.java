package com.pinchez.streams.topology.fraud;

import com.pinchez.events.EnrichedTransaction;
import com.pinchez.events.FraudAlert;
import com.pinchez.events.TransactionType;
import com.pinchez.streams.topology.CdcSources;
import org.apache.kafka.streams.kstream.KStream;

/** One fraud rule: enriched transactions (keyed by account) in, alerts (keyed by account) out. */
public sealed interface FraudRuleTopology
        permits TransferVelocityRule,
                RollingSpendRule,
                NewAccountLargeTransferRule,
                NightBettingBurstRule,
                RoundTripTransferRule {

    KStream<String, FraudAlert> detect(KStream<String, EnrichedTransaction> enriched, CdcSources sources);

    static boolean isOutgoing(EnrichedTransaction transaction) {
        return transaction.getType() == TransactionType.P2P
                || transaction.getType() == TransactionType.MERCHANT_PAYMENT;
    }
}
