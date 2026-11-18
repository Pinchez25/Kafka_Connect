package com.pinchez.wallet.transaction.dto;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.util.UUID;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = TransactionRequest.P2pTransfer.class, name = "P2P"),
    @JsonSubTypes.Type(value = TransactionRequest.TopUp.class, name = "TOPUP"),
    @JsonSubTypes.Type(value = TransactionRequest.MerchantPayment.class, name = "MERCHANT_PAYMENT"),
})
public sealed interface TransactionRequest {

    BigDecimal amount();

    record P2pTransfer(
            @NotNull UUID fromAccountId,
            @NotNull UUID toAccountId,

            @NotNull @Positive @Digits(integer = 17, fraction = 2)
            BigDecimal amount)
            implements TransactionRequest {

        @AssertTrue(message = "cannot send money to the same account")
        public boolean isDistinctAccounts() {
            return fromAccountId == null || !fromAccountId.equals(toAccountId);
        }
    }

    record TopUp(
            @NotNull UUID toAccountId,

            @NotBlank @Pattern(regexp = "^\\d{6}$", message = "must be 6 digits")
            String agentNumber,

            @NotNull @Positive @Digits(integer = 17, fraction = 2)
            BigDecimal amount)
            implements TransactionRequest {}

    record MerchantPayment(
            @NotNull UUID fromAccountId,
            @NotBlank String tillNumber,

            @NotNull @Positive @Digits(integer = 17, fraction = 2)
            BigDecimal amount)
            implements TransactionRequest {}
}
