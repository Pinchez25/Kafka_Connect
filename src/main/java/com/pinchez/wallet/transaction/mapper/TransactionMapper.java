package com.pinchez.wallet.transaction.mapper;

import com.pinchez.wallet.transaction.dto.TransactionResponse;
import com.pinchez.wallet.transaction.entity.WalletTransaction;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper
public interface TransactionMapper {

    @Mapping(target = "fromAccountId", source = "fromAccount.id")
    @Mapping(target = "toAccountId", source = "toAccount.id")
    @Mapping(target = "merchantId", source = "merchant.id")
    TransactionResponse toResponse(WalletTransaction transaction);
}
