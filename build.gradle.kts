plugins {
    base
    id("com.diffplug.spotless")
}

repositories {
    mavenCentral()
}

spotless {
    java {
        target("*/src/**/*.java")
        palantirJavaFormat(
            libs.versions.palantir.java.format
                .get(),
        ).formatJavadoc(true)
        removeUnusedImports()
        importOrder()
        forbidWildcardImports()
        trimTrailingWhitespace()
        endWithNewline()
    }
    kotlinGradle {
        target("*.gradle.kts", "*/*.gradle.kts", "buildSrc/src/**/*.gradle.kts")
        ktlint()
    }
    kotlin {
        target("buildSrc/src/**/*.kt")
        ktlint()
    }
    format("misc") {
        target(
            "*.md",
            "docs/**/*.md",
            "*.yml",
            "*/src/**/*.yaml",
            "*/src/**/*.sql",
            "*/src/**/*.avdl",
            "http/**/*.http",
            ".gitignore",
            "gradle/*.toml",
            "connect/**",
        )
        trimTrailingWhitespace()
        endWithNewline()
    }
}
