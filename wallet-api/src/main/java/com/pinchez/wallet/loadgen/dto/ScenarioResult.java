package com.pinchez.wallet.loadgen.dto;

import com.pinchez.wallet.loadgen.enums.FraudScenario;
import java.util.List;
import java.util.UUID;

public record ScenarioResult(
        FraudScenario scenario, List<UUID> accountIds, List<UUID> transactionIds, String expectation) {}
