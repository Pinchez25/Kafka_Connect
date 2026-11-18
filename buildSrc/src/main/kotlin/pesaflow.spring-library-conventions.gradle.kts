import org.springframework.boot.gradle.plugin.SpringBootPlugin

plugins {
    id("pesaflow.spring-conventions")
    `java-library`
}

dependencyManagement {
    imports {
        mavenBom(SpringBootPlugin.BOM_COORDINATES)
    }
}
