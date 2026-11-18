package com.pinchez.streams.controller;

import com.pinchez.events.AccountProfile;
import com.pinchez.events.AccountSummary;
import com.pinchez.events.Balance;
import com.pinchez.events.EnrichedTransaction;
import com.pinchez.streams.client.RemoteQueryClient;
import com.pinchez.streams.mapper.QueryResponseMapper;
import com.pinchez.streams.service.QueryResult;
import com.pinchez.streams.service.StateStoreQueryService;
import com.pinchez.streams.topology.StoreNames;
import jakarta.servlet.http.HttpServletRequest;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
class AccountStateController {

    private final StateStoreQueryService queries;
    private final RemoteQueryClient remote;

    @GetMapping("/balances/{accountId}")
    ResponseEntity<?> balance(@PathVariable UUID accountId, HttpServletRequest request) {
        return respond(
                queries.<Balance, Object>find(
                        StoreNames.BALANCES,
                        accountId,
                        Balance.class,
                        balance -> QueryResponseMapper.toResponse(accountId, balance)),
                request);
    }

    @GetMapping("/accounts/{accountId}/profile")
    ResponseEntity<?> profile(@PathVariable UUID accountId, HttpServletRequest request) {
        return respond(
                queries.<AccountProfile, Object>find(
                        StoreNames.ACCOUNT_PROFILES,
                        accountId,
                        AccountProfile.class,
                        profile -> QueryResponseMapper.toResponse(accountId, profile)),
                request);
    }

    @GetMapping("/accounts/{accountId}/summary")
    ResponseEntity<?> summary(@PathVariable UUID accountId, HttpServletRequest request) {
        return respond(
                queries.<AccountSummary, Object>find(
                        StoreNames.ACCOUNT_SUMMARIES,
                        accountId,
                        AccountSummary.class,
                        summary -> QueryResponseMapper.toResponse(accountId, summary)),
                request);
    }

    @GetMapping("/accounts/{accountId}/largest-transaction")
    ResponseEntity<?> largestTransaction(@PathVariable UUID accountId, HttpServletRequest request) {
        return respond(
                queries.<EnrichedTransaction, Object>find(
                        StoreNames.LARGEST_TRANSACTIONS,
                        accountId,
                        EnrichedTransaction.class,
                        QueryResponseMapper::toResponse),
                request);
    }

    private ResponseEntity<?> respond(QueryResult<Object> result, HttpServletRequest request) {
        return switch (result) {
            case QueryResult.Local<Object>(var value) -> ResponseEntity.ok(value);
            case QueryResult.Remote<Object>(var owner) -> remote.forward(owner, request);
        };
    }
}
