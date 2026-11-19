package com.pinchez.streams.service;

import com.pinchez.common.exception.ResourceNotFoundException;
import com.pinchez.common.exception.ServiceUnavailableException;
import java.util.UUID;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;
import org.springframework.stereotype.Service;

/** Runs a lookup against the local state store, or says which instance owns the key. */
@Service
@RequiredArgsConstructor
public class StateStoreQueryService {

    private static final StringSerializer KEY_SERIALIZER = new StringSerializer();

    private final KafkaStreamsInteractiveQueryService interactiveQueries;

    public <V, R> QueryResult<R> find(String storeName, UUID key, Class<?> resource, Function<V, R> toResponse) {
        return query(storeName, QueryableStoreTypes.<String, V>keyValueStore(), key, (store, storeKey) -> {
            var value = store.get(storeKey);
            if (value == null) {
                throw new ResourceNotFoundException(resource, key);
            }
            return toResponse.apply(value);
        });
    }

    public <S, R> QueryResult<R> query(
            String storeName, QueryableStoreType<S> storeType, UUID key, LocalLookup<S, R> lookup) {
        var storeKey = key.toString();
        try {
            var owner = interactiveQueries.getKafkaStreamsApplicationHostInfo(storeName, storeKey, KEY_SERIALIZER);
            if (!owner.equals(interactiveQueries.getCurrentKafkaStreamsApplicationHostInfo())) {
                return new QueryResult.Remote<>(owner);
            }
            var store = interactiveQueries.retrieveQueryableStore(storeName, storeType);
            return new QueryResult.Local<>(lookup.apply(store, storeKey));
        } catch (InvalidStateStoreException | IllegalStateException e) {
            throw new ServiceUnavailableException(
                    "State store '%s' is not queryable right now (starting up or rebalancing). Retry shortly."
                            .formatted(storeName));
        }
    }

    @FunctionalInterface
    public interface LocalLookup<S, R> {
        R apply(S store, String key);
    }
}
