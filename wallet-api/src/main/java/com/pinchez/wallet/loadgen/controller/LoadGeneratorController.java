package com.pinchez.wallet.loadgen.controller;

import com.pinchez.wallet.loadgen.dto.LoadGeneratorStatus;
import com.pinchez.wallet.loadgen.dto.ScenarioResult;
import com.pinchez.wallet.loadgen.enums.FraudScenario;
import com.pinchez.wallet.loadgen.service.FraudScenarioService;
import com.pinchez.wallet.loadgen.service.LoadGenerator;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/load-generator")
@RequiredArgsConstructor
class LoadGeneratorController {

    private final LoadGenerator loadGenerator;
    private final FraudScenarioService fraudScenarios;

    @GetMapping
    LoadGeneratorStatus status() {
        return loadGenerator.status();
    }

    @PostMapping("/start")
    LoadGeneratorStatus start() {
        return loadGenerator.start();
    }

    @PostMapping("/stop")
    LoadGeneratorStatus stop() {
        return loadGenerator.stop();
    }

    @PostMapping("/scenarios/{scenario}")
    ScenarioResult runScenario(@PathVariable FraudScenario scenario) {
        return fraudScenarios.run(scenario);
    }
}
