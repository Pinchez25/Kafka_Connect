package com.pinchez.wallet.transaction.repository;

import com.pinchez.wallet.transaction.entity.WalletTransaction;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface WalletTransactionRepository extends JpaRepository<WalletTransaction, UUID> {

    // Status is a literal, not a parameter, so Postgres can match the partial index under generic plans.
    @Query("""
            select coalesce(sum(t.amount), 0)
            from WalletTransaction t
            where t.fromAccount.id = :accountId
              and t.status = com.pinchez.wallet.transaction.enums.TransactionStatus.COMPLETED
              and t.createdAt >= :since
            """)
    BigDecimal sumCompletedOutgoingSince(UUID accountId, Instant since);

    @Query("""
            select t from WalletTransaction t
            where t.fromAccount.id = :accountId or t.toAccount.id = :accountId
            """)
    Page<WalletTransaction> findInvolvingAccount(UUID accountId, Pageable pageable);
}
