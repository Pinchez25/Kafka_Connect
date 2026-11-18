plugins {
    id("pesaflow.java-conventions")
}

val libs = the<VersionCatalogsExtension>().named("libs")

dependencies {
    "implementation"(libs.findLibrary("avro").get())
}

val generateAvroJava =
    tasks.register<GenerateAvroJava>("generateAvroJava") {
        description = "Generates Java source files from Avro IDL and schema files."
        source.from(fileTree("src/main/avro") { include("**/*.avdl", "**/*.avsc") })
        outputDir = layout.buildDirectory.dir("generated/sources/avro/main")
    }

sourceSets.main {
    java.srcDir(generateAvroJava)
}
