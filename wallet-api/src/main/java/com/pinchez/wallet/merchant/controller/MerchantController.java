package com.pinchez.wallet.merchant.controller;

import com.pinchez.wallet.common.dto.PageResponse;
import com.pinchez.wallet.merchant.dto.MerchantResponse;
import com.pinchez.wallet.merchant.dto.RegisterMerchantRequest;
import com.pinchez.wallet.merchant.service.MerchantService;
import jakarta.validation.Valid;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

@RestController
@RequestMapping("/merchants")
@RequiredArgsConstructor
class MerchantController {

    private final MerchantService merchantService;

    @PostMapping
    ResponseEntity<MerchantResponse> register(
            @Valid @RequestBody RegisterMerchantRequest request, UriComponentsBuilder uri) {
        var merchant = merchantService.register(request);
        return ResponseEntity.created(uri.path("/merchants/{id}").build(merchant.id()))
                .body(merchant);
    }

    @GetMapping("/{id}")
    MerchantResponse get(@PathVariable UUID id) {
        return merchantService.get(id);
    }

    @GetMapping
    PageResponse<MerchantResponse> list(@PageableDefault(sort = "id") Pageable pageable) {
        return merchantService.list(pageable);
    }
}
