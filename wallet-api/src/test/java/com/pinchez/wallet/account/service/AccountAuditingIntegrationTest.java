package com.pinchez.wallet.account.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.pinchez.TestcontainersConfiguration;
import com.pinchez.wallet.account.dto.ChangeKycLevelRequest;
import com.pinchez.wallet.account.dto.OpenAccountRequest;
import com.pinchez.wallet.account.enums.KycLevel;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class AccountAuditingIntegrationTest {

    @Autowired
    AccountService accountService;

    @Test
    void openingSetsBothTimestampsAndChangesOnlyMoveUpdatedAt() {
        var phone = "2547%08d".formatted(ThreadLocalRandom.current().nextInt(100_000_000));
        var opened = accountService.open(new OpenAccountRequest(phone, "Audit User", KycLevel.BASIC));

        assertThat(opened.createdAt()).isNotNull().isEqualTo(opened.updatedAt());

        var upgraded = accountService.changeKycLevel(opened.id(), new ChangeKycLevelRequest(KycLevel.FULL));

        assertThat(upgraded.createdAt()).isEqualTo(opened.createdAt());
        assertThat(upgraded.updatedAt()).isAfter(opened.updatedAt());
        assertThat(accountService.get(opened.id())).isEqualTo(upgraded);
    }
}
