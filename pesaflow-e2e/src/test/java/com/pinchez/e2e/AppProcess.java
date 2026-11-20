package com.pinchez.e2e;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;

/** One of the applications, started from its boot jar the way it would run in production: configured by environment. */
final class AppProcess implements AutoCloseable {

    private final String name;
    private final Process process;
    private final Path log;

    private AppProcess(String name, Process process, Path log) {
        this.name = name;
        this.process = process;
        this.log = log;
    }

    static AppProcess start(String name, Path jar, Map<String, String> environment, Path logDirectory)
            throws IOException {
        Files.createDirectories(logDirectory);
        var log = logDirectory.resolve(name + ".log");
        var java = ProcessHandle.current().info().command().orElse("java");
        var builder = new ProcessBuilder(List.of(java, "-Xmx512m", "-jar", jar.toString()))
                .redirectErrorStream(true)
                .redirectOutput(log.toFile());
        builder.environment().putAll(environment);
        return new AppProcess(name, builder.start(), log);
    }

    void awaitHealthy(Http http, int port, Duration timeout) {
        Awaitility.await(name + " healthy")
                .atMost(timeout)
                .pollInterval(Duration.ofSeconds(2))
                .ignoreExceptions()
                .until(() -> {
                    if (!process.isAlive()) {
                        throw new IllegalStateException(name + " exited; see " + log);
                    }
                    return "UP"
                            .equals(http.get(port, "/actuator/health")
                                    .path("status")
                                    .asString());
                });
    }

    @Override
    public void close() throws InterruptedException {
        process.destroy();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
        }
    }
}
