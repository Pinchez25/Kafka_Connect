plugins {
    id("pesaflow.spring-library-conventions")
}

description = "RFC 9457 ProblemDetail error handling shared by the PesaFlow web applications"

dependencies {
    api("org.springframework:spring-webmvc")
    implementation("org.springframework:spring-tx")
    implementation("org.slf4j:slf4j-api")
    compileOnly("jakarta.servlet:jakarta.servlet-api")
}
