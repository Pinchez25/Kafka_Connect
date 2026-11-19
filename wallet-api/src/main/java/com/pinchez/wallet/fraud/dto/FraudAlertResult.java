package com.pinchez.wallet.fraud.dto;

import com.pinchez.wallet.account.enums.FreezeOutcome;
import java.util.UUID;

public record FraudAlertResult(UUID alertId, UUID accountId, FreezeOutcome outcome) {}
