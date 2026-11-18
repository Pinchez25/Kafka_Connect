package com.pinchez.wallet.transaction.controller;

import com.pinchez.wallet.common.dto.PageResponse;
import com.pinchez.wallet.transaction.dto.TransactionRequest;
import com.pinchez.wallet.transaction.dto.TransactionResponse;
import com.pinchez.wallet.transaction.service.TransactionService;
import jakarta.validation.Valid;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

@RestController
@RequestMapping("/transactions")
@RequiredArgsConstructor
class TransactionController {

    private final TransactionService transactionService;

    @PostMapping
    ResponseEntity<TransactionResponse> submit(
            @Valid @RequestBody TransactionRequest request, UriComponentsBuilder uri) {
        var transaction = transactionService.submit(request);
        return ResponseEntity.created(uri.path("/transactions/{id}").build(transaction.id()))
                .body(transaction);
    }

    @GetMapping("/{id}")
    TransactionResponse get(@PathVariable UUID id) {
        return transactionService.get(id);
    }

    @GetMapping
    PageResponse<TransactionResponse> list(
            @RequestParam(required = false) @Nullable UUID accountId,
            @PageableDefault(sort = "id", direction = Sort.Direction.DESC) Pageable pageable) {
        return transactionService.list(accountId, pageable);
    }
}
