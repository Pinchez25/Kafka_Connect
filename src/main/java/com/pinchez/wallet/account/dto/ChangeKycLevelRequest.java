package com.pinchez.wallet.account.dto;

import com.pinchez.wallet.account.enums.KycLevel;
import jakarta.validation.constraints.NotNull;

public record ChangeKycLevelRequest(@NotNull KycLevel kycLevel) {}
