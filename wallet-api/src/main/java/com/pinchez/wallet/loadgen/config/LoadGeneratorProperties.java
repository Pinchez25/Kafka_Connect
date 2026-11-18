package com.pinchez.wallet.loadgen.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("pesaflow.load-generator")
public record LoadGeneratorProperties(
        boolean enabled,
        @NotNull Duration interval,
        @Positive int transactionsPerTick,
        @Min(2) int seedAccounts,
        @Min(1) int seedMerchants,
        @DecimalMin("0.0") @DecimalMax("1.0") double newAccountProbability,
        @NotNull @Valid Mix mix) {

    public record Mix(
            @PositiveOrZero int p2p,
            @PositiveOrZero int merchantPayment,
            @PositiveOrZero int topUp) {

        public int total() {
            return p2p + merchantPayment + topUp;
        }
    }
}
