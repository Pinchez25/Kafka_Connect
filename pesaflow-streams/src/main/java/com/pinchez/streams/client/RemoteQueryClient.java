package com.pinchez.streams.client;

import com.pinchez.common.exception.ServiceUnavailableException;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

/** Relays an interactive query to the instance that owns the key. */
@Component
public class RemoteQueryClient {

    static final String FORWARDED_HEADER = "X-Pesaflow-Forwarded";

    private final RestClient restClient = RestClient.create();

    public ResponseEntity<byte[]> forward(HostInfo owner, HttpServletRequest request) {
        // A forwarded request that lands on a non-owner means ownership moved mid-flight; don't bounce it around.
        if (request.getHeader(FORWARDED_HEADER) != null) {
            throw new ServiceUnavailableException("Partition ownership is changing. Retry shortly.");
        }
        var uri = UriComponentsBuilder.newInstance()
                .scheme("http")
                .host(owner.host())
                .port(owner.port())
                .path(request.getRequestURI())
                .query(request.getQueryString())
                .build(true)
                .toUri();
        return restClient
                .get()
                .uri(uri)
                .header(FORWARDED_HEADER, "true")
                .exchange((_, response) -> ResponseEntity.status(response.getStatusCode())
                        .contentType(response.getHeaders().getContentType())
                        .body(response.getBody().readAllBytes()));
    }
}
