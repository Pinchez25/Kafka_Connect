package com.pinchez.wallet.merchant.service;

import com.pinchez.common.exception.DuplicateResourceException;
import com.pinchez.common.exception.ResourceNotFoundException;
import com.pinchez.wallet.common.dto.PageResponse;
import com.pinchez.wallet.merchant.dto.MerchantResponse;
import com.pinchez.wallet.merchant.dto.RegisterMerchantRequest;
import com.pinchez.wallet.merchant.entity.Merchant;
import com.pinchez.wallet.merchant.enums.MerchantCategory;
import com.pinchez.wallet.merchant.mapper.MerchantMapper;
import com.pinchez.wallet.merchant.repository.MerchantRepository;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class MerchantService {

    private final MerchantRepository merchants;
    private final MerchantMapper mapper;

    @Transactional
    public MerchantResponse register(RegisterMerchantRequest request) {
        if (merchants.existsByTillNumber(request.tillNumber())) {
            throw new DuplicateResourceException(Merchant.class, "tillNumber", request.tillNumber());
        }
        var merchant = Merchant.register(request.tillNumber(), request.name(), request.category());
        return mapper.toResponse(merchants.save(merchant));
    }

    public MerchantResponse get(UUID id) {
        return merchants
                .findById(id)
                .map(mapper::toResponse)
                .orElseThrow(() -> new ResourceNotFoundException(Merchant.class, id));
    }

    public PageResponse<MerchantResponse> list(Pageable pageable) {
        return PageResponse.from(merchants.findAll(pageable).map(mapper::toResponse));
    }

    public Optional<MerchantResponse> anyInCategory(MerchantCategory category) {
        return merchants.findFirstByCategory(category).map(mapper::toResponse);
    }

    public List<String> allTillNumbers() {
        return merchants.findAllTillNumbers();
    }
}
