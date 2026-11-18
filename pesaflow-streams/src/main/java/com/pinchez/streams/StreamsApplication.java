package com.pinchez.streams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication(scanBasePackages = {"com.pinchez.streams", "com.pinchez.common"})
@ConfigurationPropertiesScan
public class StreamsApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamsApplication.class, args);
    }
}
