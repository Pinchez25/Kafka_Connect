package com.pinchez.wallet.merchant.dto;

import com.pinchez.wallet.merchant.enums.MerchantCategory;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record RegisterMerchantRequest(
        @NotBlank @Pattern(regexp = "^\\d{5,7}$", message = "must be 5 to 7 digits")
        String tillNumber,

        @NotBlank @Size(max = 120) String name,
        @NotNull MerchantCategory category) {}
