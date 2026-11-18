package com.pinchez.wallet.merchant.entity;

import com.pinchez.wallet.merchant.enums.MerchantCategory;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.UuidGenerator;

@Entity
@Table(name = "merchants")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Merchant {

    @Id
    @GeneratedValue
    @UuidGenerator(style = UuidGenerator.Style.VERSION_7)
    private UUID id;

    @Column(nullable = false, unique = true, length = 10)
    private String tillNumber;

    @Column(nullable = false, length = 120)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private MerchantCategory category;

    @Column(nullable = false)
    private Instant createdAt;

    public static Merchant register(String tillNumber, String name, MerchantCategory category, Instant now) {
        var merchant = new Merchant();
        merchant.tillNumber = tillNumber;
        merchant.name = name;
        merchant.category = category;
        merchant.createdAt = now;
        return merchant;
    }
}
