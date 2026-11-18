package com.pinchez.streams.service;

import com.pinchez.common.exception.ResourceNotFoundException;
import com.pinchez.common.exception.ServiceUnavailableException;
import java.util.UUID;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;
import org.springframework.stereotype.Service;

/** Looks a key up in the local state store, or says which instance owns it. */
@Service
@RequiredArgsConstructor
public class StateStoreQueryService {

    private static final StringSerializer KEY_SERIALIZER = new StringSerializer();

    private final KafkaStreamsInteractiveQueryService interactiveQueries;

    public <V, R> QueryResult<R> find(String storeName, UUID key, Class<?> resource, Function<V, R> toResponse) {
        var storeKey = key.toString();
        try {
            var owner = interactiveQueries.getKafkaStreamsApplicationHostInfo(storeName, storeKey, KEY_SERIALIZER);
            if (!owner.equals(interactiveQueries.getCurrentKafkaStreamsApplicationHostInfo())) {
                return new QueryResult.Remote<>(owner);
            }
            ReadOnlyKeyValueStore<String, V> store =
                    interactiveQueries.retrieveQueryableStore(storeName, QueryableStoreTypes.keyValueStore());
            var value = store.get(storeKey);
            if (value == null) {
                throw new ResourceNotFoundException(resource, key);
            }
            return new QueryResult.Local<>(toResponse.apply(value));
        } catch (InvalidStateStoreException | IllegalStateException e) {
            throw new ServiceUnavailableException(
                    "State store '%s' is not queryable right now (starting up or rebalancing). Retry shortly."
                            .formatted(storeName));
        }
    }
}
