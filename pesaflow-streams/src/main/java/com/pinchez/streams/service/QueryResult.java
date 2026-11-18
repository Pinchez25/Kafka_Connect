package com.pinchez.streams.service;

import org.apache.kafka.streams.state.HostInfo;

public sealed interface QueryResult<T> {

    record Local<T>(T value) implements QueryResult<T> {}

    record Remote<T>(HostInfo owner) implements QueryResult<T> {}
}
