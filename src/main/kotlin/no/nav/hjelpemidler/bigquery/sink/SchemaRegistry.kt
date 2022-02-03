package no.nav.hjelpemidler.bigquery.sink

import com.fasterxml.jackson.module.kotlin.readValue
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardSQLTypeName
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo
import io.ktor.util.extension
import mu.KotlinLogging
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.inputStream
import kotlin.streams.asSequence

class SchemaRegistry(private val tables: Map<String, RegistryTable>) : Map<String, RegistryTable> by tables {

    companion object {
        private val log = KotlinLogging.logger {}

        fun create(schemaDirName: String): SchemaRegistry {
            val schemaDir = checkNotNull(SchemaRegistry::class.java.getResource(schemaDirName)) {
                "Resource '$schemaDirName' not found"
            }.toURI()

            log.info { "Creating registry tables from files in directory: '$schemaDir'" }

            val path: Path = when (schemaDir.scheme) {
                "jar" -> FileSystems
                    .newFileSystem(schemaDir, emptyMap<String, String>())
                    .getPath(schemaDirName)
                else -> Paths.get(schemaDir)
            }

            val tables = Files.walk(path)
                .asSequence()
                .filter { it.extension == "yaml" }
                .map {
                    log.info { "Creating registry table for file: '$it'" }
                    yamlMapper.readValue<RegistryTable>(it.inputStream())
                }
                .associateBy { it.name }

            return SchemaRegistry(tables)
        }
    }
}

data class RegistryTable(
    val name: String,
    val description: String?,
    val fields: List<RegistryField> = emptyList(),
) {
    fun toTableInfo(datasetId: DatasetId): TableInfo {
        val schema = fields.map { it.toField() }.let { Schema.of(it) }
        val tableDefinition = StandardTableDefinition.of(schema)
        val tableId = TableId.of(datasetId.project, datasetId.dataset, name)
        return TableInfo.newBuilder(tableId, tableDefinition)
            .setDescription(description)
            .build()
    }
}

data class RegistryField(
    val name: String,
    val type: StandardSQLTypeName,
    val mode: Mode = Mode.NULLABLE,
    val description: String?,
    val maxLength: String?,
) {
    fun toField(): Field = Field.newBuilder(name, type)
        .setMode(mode)
        .setDescription(description)
        .setMaxLength(maxLength?.toLongOrNull())
        .build()
}
