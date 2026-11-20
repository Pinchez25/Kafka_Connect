# syntax=docker/dockerfile:1.7
# One image per application module, built from the repository root:
#   docker build --build-arg MODULE=wallet-api -t pesaflow/wallet-api .
#   docker build --build-arg MODULE=pesaflow-streams --build-arg PORT=8082 -t pesaflow/pesaflow-streams .

FROM eclipse-temurin:25-jdk-noble AS build
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle gradle
COPY buildSrc buildSrc
# Build scripts before sources: Gradle needs every included project's directory, and this layer (wrapper download,
# buildSrc compilation) is reused until a build script changes.
COPY common-web/build.gradle.kts common-web/
COPY wallet-api/build.gradle.kts wallet-api/
COPY pesaflow-streams/build.gradle.kts pesaflow-streams/
COPY pesaflow-e2e/build.gradle.kts pesaflow-e2e/
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon --quiet help
COPY common-web common-web
COPY wallet-api wallet-api
COPY pesaflow-streams pesaflow-streams
ARG MODULE
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon --quiet ":${MODULE}:bootJar" \
    && find "${MODULE}/build/libs" -name '*.jar' ! -name '*-plain.jar' -exec cp {} /workspace/app.jar \;

FROM eclipse-temurin:25-jre-noble AS layers
WORKDIR /layers
COPY --from=build /workspace/app.jar app.jar
RUN java -Djarmode=tools -jar app.jar extract --layers --destination extracted

# glibc (not Alpine/musl): RocksDB's native library, used by Kafka Streams state stores, needs it.
FROM eclipse-temurin:25-jre-noble
ARG PORT=8080
RUN groupadd --system --gid 10001 pesaflow \
    && useradd --system --uid 10001 --gid pesaflow --no-create-home --shell /usr/sbin/nologin pesaflow \
    && mkdir -p /var/lib/pesaflow \
    && chown pesaflow:pesaflow /var/lib/pesaflow
WORKDIR /app
# Least to most frequently changing, so a code change only rebuilds the last layer.
COPY --from=layers /layers/extracted/dependencies/ ./
COPY --from=layers /layers/extracted/spring-boot-loader/ ./
COPY --from=layers /layers/extracted/snapshot-dependencies/ ./
COPY --from=layers /layers/extracted/application/ ./
USER 10001:10001
ENV SERVER_PORT=${PORT} \
    JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
EXPOSE ${PORT}
ENTRYPOINT ["java", "-jar", "app.jar"]
