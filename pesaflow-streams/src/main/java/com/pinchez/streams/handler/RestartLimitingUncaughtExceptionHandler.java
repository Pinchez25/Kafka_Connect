package com.pinchez.streams.handler;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

/**
 * Replaces a dead stream thread, which recovers from transient failures. A failure that keeps coming back (a poison
 * record with a fail policy) would replace threads forever, so after {@code maxReplacements} within {@code window} the
 * whole client shuts down instead.
 */
@Slf4j
public class RestartLimitingUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {

    private final int maxReplacements;
    private final Duration window;
    private final Clock clock;
    private final Deque<Instant> replacements = new ArrayDeque<>();

    public RestartLimitingUncaughtExceptionHandler(int maxReplacements, Duration window, Clock clock) {
        this.maxReplacements = maxReplacements;
        this.window = window;
        this.clock = clock;
    }

    @Override
    public synchronized StreamThreadExceptionResponse handle(Throwable exception) {
        var now = clock.instant();
        while (!replacements.isEmpty() && replacements.peekFirst().isBefore(now.minus(window))) {
            replacements.removeFirst();
        }
        if (replacements.size() >= maxReplacements) {
            log.error(
                    "Stream thread failed {} times within {}; shutting down the client",
                    replacements.size() + 1,
                    window,
                    exception);
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        }
        replacements.addLast(now);
        log.error("Stream thread failed; replacing it ({} of {})", replacements.size(), maxReplacements, exception);
        return StreamThreadExceptionResponse.REPLACE_THREAD;
    }
}
