import org.apache.avro.Schema
import org.apache.avro.compiler.specific.SpecificCompiler
import org.apache.avro.generic.GenericData
import org.apache.avro.idl.IdlReader
import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.SkipWhenEmpty
import org.gradle.api.tasks.TaskAction

@CacheableTask
abstract class GenerateAvroJava : DefaultTask() {
    @get:InputFiles
    @get:SkipWhenEmpty
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val source: ConfigurableFileCollection

    @get:OutputDirectory
    abstract val outputDir: DirectoryProperty

    @TaskAction
    fun generate() {
        val destination = outputDir.get().asFile
        destination.deleteRecursively()
        destination.mkdirs()

        val avscParser = Schema.Parser()
        val schemas =
            source.files
                .sortedBy { it.path }
                .flatMap { file ->
                    when (file.extension) {
                        "avdl" -> IdlReader().parse(file.toPath()).namedSchemas.values
                        else -> listOf(avscParser.parse(file))
                    }
                }.associateBy { it.fullName }
                .values

        SpecificCompiler(schemas)
            .apply {
                setStringType(GenericData.StringType.String)
                setEnableDecimalLogicalType(true)
                setFieldVisibility(SpecificCompiler.FieldVisibility.PRIVATE)
                setCreateSetters(false)
                setGettersReturnOptional(true)
                setOptionalGettersForNullableFieldsOnly(true)
                setOutputCharacterEncoding("UTF-8")
            }.compileToDestination(null, destination)
    }
}
