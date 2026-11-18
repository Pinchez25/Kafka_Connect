package com.pinchez.wallet.account.mapper;

import com.pinchez.wallet.account.dto.AccountResponse;
import com.pinchez.wallet.account.entity.Account;
import org.mapstruct.Mapper;

@Mapper
public interface AccountMapper {

    AccountResponse toResponse(Account account);
}
