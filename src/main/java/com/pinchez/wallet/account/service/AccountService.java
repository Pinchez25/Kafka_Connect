package com.pinchez.wallet.account.service;

import com.pinchez.common.dto.PageResponse;
import com.pinchez.common.exception.DuplicateResourceException;
import com.pinchez.common.exception.ResourceNotFoundException;
import com.pinchez.wallet.account.dto.AccountResponse;
import com.pinchez.wallet.account.dto.ChangeAccountStatusRequest;
import com.pinchez.wallet.account.dto.ChangeKycLevelRequest;
import com.pinchez.wallet.account.dto.OpenAccountRequest;
import com.pinchez.wallet.account.entity.Account;
import com.pinchez.wallet.account.mapper.AccountMapper;
import com.pinchez.wallet.account.repository.AccountRepository;
import com.pinchez.wallet.config.WalletProperties;
import java.time.Clock;
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
    private final Clock clock;

    @Transactional
    public AccountResponse open(OpenAccountRequest request) {
        if (accounts.existsByPhoneNumber(request.phoneNumber())) {
            throw new DuplicateResourceException(Account.class, "phoneNumber", request.phoneNumber());
        }
        var account = Account.open(
                request.phoneNumber(),
                request.fullName(),
                request.kycLevel(),
                walletProperties.dailyLimitFor(request.kycLevel()),
                clock.instant());
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
        account.changeStatus(request.status(), clock.instant());
        return mapper.toResponse(account);
    }

    @Transactional
    public AccountResponse changeKycLevel(UUID id, ChangeKycLevelRequest request) {
        var account = require(id);
        account.changeKycLevel(request.kycLevel(), walletProperties.dailyLimitFor(request.kycLevel()), clock.instant());
        return mapper.toResponse(account);
    }

    private Account require(UUID id) {
        return accounts.findById(id).orElseThrow(() -> new ResourceNotFoundException(Account.class, id));
    }
}
