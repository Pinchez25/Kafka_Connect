package com.pinchez.streams.service;

import com.pinchez.streams.dto.AccountActivityResponse;
import com.pinchez.streams.topology.StoreNames;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class AccountActivityService {

    private final StateStoreQueryService queries;

    /** Transaction counts for every window that starts within [from, to]. */
    public QueryResult<AccountActivityResponse> activity(UUID accountId, Instant from, Instant to) {
        return queries.query(
                StoreNames.ACCOUNT_ACTIVITY,
                QueryableStoreTypes.<String, Long>windowStore(),
                accountId,
                (store, key) -> {
                    var windows = new ArrayList<AccountActivityResponse.WindowCount>();
                    try (var iterator = store.fetch(key, from, to)) {
                        iterator.forEachRemaining(window -> windows.add(new AccountActivityResponse.WindowCount(
                                Instant.ofEpochMilli(window.key), window.value)));
                    }
                    return new AccountActivityResponse(accountId, from, to, windows);
                });
    }
}
