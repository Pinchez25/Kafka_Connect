plugins {
    id("pesaflow.spring-library-conventions")
}

description = "End-to-end test: Postgres, Kafka, Schema Registry, Connect and both applications, all real"

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core")
    testImplementation("org.awaitility:awaitility")
    testImplementation("org.apache.kafka:kafka-clients")
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testImplementation("tools.jackson.core:jackson-databind")
    testRuntimeOnly("ch.qos.logback:logback-classic")
}

val wallet = project(":wallet-api")
val streams = project(":pesaflow-streams")

// Minutes of containers and two JVMs: run on demand with ./gradlew :pesaflow-e2e:e2eTest, not on every build.
tasks.test {
    enabled = false
}

tasks.register<Test>("e2eTest") {
    description = "Runs the whole pipeline end to end against real containers."
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    testClassesDirs =
        sourceSets.test
            .get()
            .output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    dependsOn(":wallet-api:bootJar", ":pesaflow-streams:bootJar")
    systemProperty("pesaflow.e2e.root", rootDir.absolutePath)
    systemProperty(
        "pesaflow.e2e.wallet-jar",
        wallet.layout.buildDirectory
            .file("libs/wallet-api-$version.jar")
            .get()
            .asFile,
    )
    systemProperty(
        "pesaflow.e2e.streams-jar",
        streams.layout.buildDirectory
            .file("libs/pesaflow-streams-$version.jar")
            .get()
            .asFile,
    )
    systemProperty(
        "pesaflow.e2e.logs",
        layout.buildDirectory
            .dir("e2e-logs")
            .get()
            .asFile,
    )
    outputs.upToDateWhen { false }
}
