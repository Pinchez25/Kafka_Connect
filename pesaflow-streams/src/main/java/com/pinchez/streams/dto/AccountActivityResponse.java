package com.pinchez.streams.dto;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record AccountActivityResponse(UUID accountId, Instant from, Instant to, List<WindowCount> windows) {

    public record WindowCount(Instant windowStart, long transactions) {}
}
