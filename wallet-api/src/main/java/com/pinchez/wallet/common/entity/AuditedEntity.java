package com.pinchez.wallet.common.entity;

import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import java.time.Instant;
import lombok.Getter;
import org.springframework.data.annotation.LastModifiedDate;

@Getter
@MappedSuperclass
public abstract class AuditedEntity extends CreationAuditedEntity {

    @LastModifiedDate
    @Column(nullable = false)
    private Instant updatedAt;
}
