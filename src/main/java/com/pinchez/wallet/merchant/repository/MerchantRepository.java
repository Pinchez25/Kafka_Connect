package com.pinchez.wallet.merchant.repository;

import com.pinchez.wallet.merchant.entity.Merchant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface MerchantRepository extends JpaRepository<Merchant, UUID> {

    boolean existsByTillNumber(String tillNumber);

    Optional<Merchant> findByTillNumber(String tillNumber);

    @Query("select m.tillNumber from Merchant m")
    List<String> findAllTillNumbers();
}
