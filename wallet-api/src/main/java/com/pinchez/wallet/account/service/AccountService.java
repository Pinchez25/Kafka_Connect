package com.pinchez.wallet.account.service;

import com.pinchez.common.exception.DuplicateResourceException;
import com.pinchez.common.exception.ResourceNotFoundException;
import com.pinchez.wallet.account.dto.AccountResponse;
import com.pinchez.wallet.account.dto.ChangeAccountStatusRequest;
import com.pinchez.wallet.account.dto.ChangeKycLevelRequest;
import com.pinchez.wallet.account.dto.OpenAccountRequest;
import com.pinchez.wallet.account.entity.Account;
import com.pinchez.wallet.account.enums.AccountStatus;
import com.pinchez.wallet.account.enums.FreezeOutcome;
import com.pinchez.wallet.account.mapper.AccountMapper;
import com.pinchez.wallet.account.repository.AccountRepository;
import com.pinchez.wallet.common.dto.PageResponse;
import com.pinchez.wallet.config.WalletProperties;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class AccountService {

    private final AccountRepository accounts;
    private final AccountMapper mapper;
    private final WalletProperties walletProperties;

    @Transactional
    public AccountResponse open(OpenAccountRequest request) {
        if (accounts.existsByPhoneNumber(request.phoneNumber())) {
            throw new DuplicateResourceException(Account.class, "phoneNumber", request.phoneNumber());
        }
        var account = Account.open(
                request.phoneNumber(),
                request.fullName(),
                request.kycLevel(),
                walletProperties.dailyLimitFor(request.kycLevel()));
        return mapper.toResponse(accounts.save(account));
    }

    public AccountResponse get(UUID id) {
        return mapper.toResponse(require(id));
    }

    public PageResponse<AccountResponse> list(Pageable pageable) {
        return PageResponse.from(accounts.findAll(pageable).map(mapper::toResponse));
    }

    public List<UUID> allIds() {
        return accounts.findAllIds();
    }

    @Transactional
    public AccountResponse changeStatus(UUID id, ChangeAccountStatusRequest request) {
        var account = require(id);
        account.changeStatus(request.status());
        return mapper.toResponse(accounts.saveAndFlush(account));
    }

    @Transactional
    public AccountResponse changeKycLevel(UUID id, ChangeKycLevelRequest request) {
        var account = require(id);
        account.changeKycLevel(request.kycLevel(), walletProperties.dailyLimitFor(request.kycLevel()));
        return mapper.toResponse(accounts.saveAndFlush(account));
    }

    /** Idempotent, so a redelivered alert is harmless. */
    @Transactional
    public FreezeOutcome freeze(UUID id) {
        return accounts.findById(id)
                .map(account -> {
                    if (account.getStatus() == AccountStatus.FROZEN) {
                        return FreezeOutcome.ALREADY_FROZEN;
                    }
                    account.changeStatus(AccountStatus.FROZEN);
                    return FreezeOutcome.FROZEN;
                })
                .orElse(FreezeOutcome.ACCOUNT_NOT_FOUND);
    }

    private Account require(UUID id) {
        return accounts.findById(id).orElseThrow(() -> new ResourceNotFoundException(Account.class, id));
    }
}
