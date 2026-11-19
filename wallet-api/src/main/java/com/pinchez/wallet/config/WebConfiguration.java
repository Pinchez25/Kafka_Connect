package com.pinchez.wallet.config;

import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration(proxyBeanMethods = false)
class WebConfiguration implements WebMvcConfigurer {

    /** Lets path and query enums be written as {@code velocity-burst} as well as {@code VELOCITY_BURST}. */
    @Override
    public void addFormatters(FormatterRegistry registry) {
        ApplicationConversionService.addApplicationConverters(registry);
    }
}
