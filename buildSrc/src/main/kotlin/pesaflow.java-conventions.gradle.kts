plugins {
    java
}

group = "com.pinchez"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/") {
        content {
            includeGroupAndSubgroups("io.confluent")
        }
    }
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}
