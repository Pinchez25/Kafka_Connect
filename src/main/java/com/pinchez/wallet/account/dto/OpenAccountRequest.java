package com.pinchez.wallet.account.dto;

import com.pinchez.wallet.account.enums.KycLevel;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record OpenAccountRequest(
        @NotBlank @Pattern(regexp = "^254[17]\\d{8}$", message = "must be a Kenyan MSISDN like 254712345678")
        String phoneNumber,

        @NotBlank @Size(max = 120) String fullName,
        KycLevel kycLevel) {

    public OpenAccountRequest {
        if (kycLevel == null) {
            kycLevel = KycLevel.BASIC;
        }
    }
}
