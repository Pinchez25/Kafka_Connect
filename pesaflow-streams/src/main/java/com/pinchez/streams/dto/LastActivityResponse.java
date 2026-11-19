package com.pinchez.streams.dto;

import java.time.Instant;
import java.util.UUID;

public record LastActivityResponse(
        UUID accountId, UUID lastTransactionId, Instant lastActivityAt, Instant reportedDormantAt) {}
