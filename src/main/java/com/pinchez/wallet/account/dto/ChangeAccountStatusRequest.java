package com.pinchez.wallet.account.dto;

import com.pinchez.wallet.account.enums.AccountStatus;
import jakarta.validation.constraints.NotNull;

public record ChangeAccountStatusRequest(@NotNull AccountStatus status) {}
