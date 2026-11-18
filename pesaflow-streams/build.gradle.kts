plugins {
    id("pesaflow.spring-boot-conventions")
    id("pesaflow.avro-conventions")
}

description = "Kafka Streams processing of wallet CDC events"

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.apache.kafka:kafka-streams")
    implementation(libs.confluent.kafka.streams.avro.serde)

    testImplementation("org.springframework.boot:spring-boot-starter-kafka-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.apache.kafka:kafka-streams-test-utils")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("org.awaitility:awaitility")
}
