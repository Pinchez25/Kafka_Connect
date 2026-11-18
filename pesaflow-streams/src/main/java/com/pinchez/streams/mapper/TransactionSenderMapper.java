package com.pinchez.streams.mapper;

import com.pinchez.cdc.Account;
import com.pinchez.events.CompletedTransaction;
import com.pinchez.events.TransactionSender;

public final class TransactionSenderMapper {

    private TransactionSenderMapper() {}

    public static TransactionSender toTransactionSender(CompletedTransaction transaction, Account sender) {
        return TransactionSender.newBuilder()
                .setTransactionId(transaction.getTransactionId())
                .setType(transaction.getType())
                .setAmount(transaction.getAmount())
                .setSenderAccountId(transaction.getFromAccountId().orElseThrow())
                .setSenderKycLevel(AccountStateMapper.kycLevel(sender))
                .setSenderStatus(AccountStateMapper.status(sender))
                .build();
    }
}
