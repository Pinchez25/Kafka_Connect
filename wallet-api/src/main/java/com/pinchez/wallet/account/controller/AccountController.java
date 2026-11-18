package com.pinchez.wallet.account.controller;

import com.pinchez.common.dto.PageResponse;
import com.pinchez.wallet.account.dto.AccountResponse;
import com.pinchez.wallet.account.dto.ChangeAccountStatusRequest;
import com.pinchez.wallet.account.dto.ChangeKycLevelRequest;
import com.pinchez.wallet.account.dto.OpenAccountRequest;
import com.pinchez.wallet.account.service.AccountService;
import jakarta.validation.Valid;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

@RestController
@RequestMapping("/accounts")
@RequiredArgsConstructor
class AccountController {

    private final AccountService accountService;

    @PostMapping
    ResponseEntity<AccountResponse> open(@Valid @RequestBody OpenAccountRequest request, UriComponentsBuilder uri) {
        var account = accountService.open(request);
        return ResponseEntity.created(uri.path("/accounts/{id}").build(account.id()))
                .body(account);
    }

    @GetMapping("/{id}")
    AccountResponse get(@PathVariable UUID id) {
        return accountService.get(id);
    }

    @GetMapping
    PageResponse<AccountResponse> list(@PageableDefault(sort = "id") Pageable pageable) {
        return accountService.list(pageable);
    }

    @PutMapping("/{id}/status")
    AccountResponse changeStatus(@PathVariable UUID id, @Valid @RequestBody ChangeAccountStatusRequest request) {
        return accountService.changeStatus(id, request);
    }

    @PutMapping("/{id}/kyc-level")
    AccountResponse changeKycLevel(@PathVariable UUID id, @Valid @RequestBody ChangeKycLevelRequest request) {
        return accountService.changeKycLevel(id, request);
    }
}
