package com.pinchez.wallet.config;

import com.pinchez.wallet.account.enums.KycLevel;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Currency;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("pesaflow.wallet")
public record WalletProperties(
        @NotNull Currency currency,
        @NotNull ZoneId zoneId,
        @NotNull Map<KycLevel, @NotNull @Positive BigDecimal> dailyLimits) {

    public WalletProperties {
        dailyLimits = dailyLimits == null
                ? Map.of()
                : dailyLimits.entrySet().stream()
                        .collect(Collectors.toUnmodifiableMap(
                                Map.Entry::getKey, entry -> entry.getValue().setScale(2, RoundingMode.HALF_UP)));
    }

    public BigDecimal dailyLimitFor(KycLevel kycLevel) {
        return dailyLimits.get(kycLevel);
    }

    @AssertTrue(message = "must define a limit for every KYC level")
    public boolean isEveryKycLevelLimited() {
        return Arrays.stream(KycLevel.values()).allMatch(dailyLimits::containsKey);
    }
}
