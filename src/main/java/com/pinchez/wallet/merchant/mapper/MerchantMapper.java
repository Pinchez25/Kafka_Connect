package com.pinchez.wallet.merchant.mapper;

import com.pinchez.wallet.merchant.dto.MerchantResponse;
import com.pinchez.wallet.merchant.entity.Merchant;
import org.mapstruct.Mapper;

@Mapper
public interface MerchantMapper {

    MerchantResponse toResponse(Merchant merchant);
}
