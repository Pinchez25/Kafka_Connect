package com.pinchez.wallet.fraud.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.UUID;

/** The parts of a streams FraudAlert the wallet acts on; the HTTP sink posts the whole record as JSON. */
public record FraudAlertNotification(
        @NotNull UUID alertId,
        @NotBlank String rule,
        @NotNull UUID accountId,
        String description) {}
