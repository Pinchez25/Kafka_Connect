package com.pinchez.wallet.common.service;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.stereotype.Component;

/**
 * Supplies audit timestamps from the application {@link Clock}. Auditing always overwrites {@code @CreatedDate}, so
 * backdated rows are written inside {@link #at(Instant, Supplier)} instead.
 */
@Component
@RequiredArgsConstructor
public class AuditClock implements DateTimeProvider {

    private static final ScopedValue<Instant> OVERRIDE = ScopedValue.newInstance();

    private final Clock clock;

    public <T> T at(Instant instant, Supplier<T> action) {
        return ScopedValue.where(OVERRIDE, instant).call(action::get);
    }

    @Override
    public Optional<TemporalAccessor> getNow() {
        return Optional.of(OVERRIDE.orElse(clock.instant()));
    }
}
