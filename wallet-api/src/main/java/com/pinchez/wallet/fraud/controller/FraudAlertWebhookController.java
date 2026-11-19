package com.pinchez.wallet.fraud.controller;

import com.pinchez.wallet.fraud.dto.FraudAlertNotification;
import com.pinchez.wallet.fraud.dto.FraudAlertResult;
import com.pinchez.wallet.fraud.service.FraudAlertService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Called by the pesaflow-fraud-webhook HTTP sink connector for every record in pesaflow.fraud.alerts. */
@RestController
@RequestMapping("/webhooks")
@RequiredArgsConstructor
class FraudAlertWebhookController {

    private final FraudAlertService fraudAlertService;

    @PostMapping("/fraud-alerts")
    FraudAlertResult fraudAlert(@Valid @RequestBody FraudAlertNotification alert) {
        return fraudAlertService.handle(alert);
    }
}
