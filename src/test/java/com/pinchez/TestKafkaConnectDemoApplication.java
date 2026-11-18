package com.pinchez;

import org.springframework.boot.SpringApplication;

public class TestKafkaConnectDemoApplication {

    public static void main(String[] args) {
        SpringApplication.from(KafkaConnectDemoApplication::main)
                .with(TestcontainersConfiguration.class)
                .run(args);
    }
}
