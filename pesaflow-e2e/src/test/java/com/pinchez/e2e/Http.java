package com.pinchez.e2e;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

final class Http {

    private final HttpClient client = HttpClient.newHttpClient();
    private final JsonMapper json = JsonMapper.builder().build();

    JsonNode get(int port, String path) throws IOException, InterruptedException {
        return get(uri(port, path).toString());
    }

    JsonNode get(String url) throws IOException, InterruptedException {
        return send(HttpRequest.newBuilder(URI.create(url)).GET());
    }

    JsonNode post(int port, String path) throws IOException, InterruptedException {
        return send(HttpRequest.newBuilder(uri(port, path)).POST(HttpRequest.BodyPublishers.noBody()));
    }

    JsonNode put(String url, String body) throws IOException, InterruptedException {
        return send(HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body)));
    }

    JsonNode read(String body) {
        return json.readTree(body);
    }

    private JsonNode send(HttpRequest.Builder request) throws IOException, InterruptedException {
        var response = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 300) {
            throw new IOException(
                    "HTTP %d from %s: %s".formatted(response.statusCode(), response.uri(), response.body()));
        }
        return json.readTree(response.body());
    }

    private static URI uri(int port, String path) {
        return URI.create("http://localhost:" + port + path);
    }
}
