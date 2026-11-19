package com.pinchez.wallet.fraud.service;

import com.pinchez.wallet.account.service.AccountService;
import com.pinchez.wallet.fraud.dto.FraudAlertNotification;
import com.pinchez.wallet.fraud.dto.FraudAlertResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Every alert freezes its account. An unknown account is reported rather than rejected: answering with an error would
 * make the HTTP sink retry an alert that can never succeed.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FraudAlertService {

    private final AccountService accountService;

    public FraudAlertResult handle(FraudAlertNotification alert) {
        var outcome = accountService.freeze(alert.accountId());
        switch (outcome) {
            case FROZEN ->
                log.info(
                        "Froze account {} after {} alert {}: {}",
                        alert.accountId(),
                        alert.rule(),
                        alert.alertId(),
                        alert.description());
            case ALREADY_FROZEN ->
                log.debug("Account {} already frozen; ignoring alert {}", alert.accountId(), alert.alertId());
            case ACCOUNT_NOT_FOUND -> log.warn("Alert {} names unknown account {}", alert.alertId(), alert.accountId());
        }
        return new FraudAlertResult(alert.alertId(), alert.accountId(), outcome);
    }
}
