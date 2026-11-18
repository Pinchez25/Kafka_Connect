package com.pinchez.streams.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("pesaflow.streams")
public record StreamsProperties(
        @NotBlank String schemaRegistryUrl,
        @NotNull @Valid Topics topics,
        @NotNull @Valid OutputTopics outputTopics) {

    public record Topics(
            @NotBlank String cdcTransactions,
            @NotBlank String p2pTransfers,
            @NotBlank String topUps,
            @NotBlank String merchantPayments) {}

    public record OutputTopics(
            @Positive int partitions, @Positive short replicationFactor) {}
}
