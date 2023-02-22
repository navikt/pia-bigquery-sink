package no.nav.pia.bigquery.sink.schema

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo

interface SchemaDefinition {
    val schemaId: Id

    fun entry(): Pair<Id, SchemaDefinition> = schemaId to this

    fun define(): TableDefinition

    fun transform(payload: JsonNode): RowToInsert

    fun skip(payload: JsonNode): Boolean = false

    fun toTableInfo(datasetId: DatasetId): TableInfo = TableInfo
        .newBuilder(schemaId.toTableId(datasetId), define())
        .build()

    data class Id(val name: String, val version: Int) {

        fun toTableId(datasetId: DatasetId): TableId = TableId.of(
            datasetId.project,
            datasetId.dataset,
            listOf(name, version).joinToString(SEPARATOR),
        )

        companion object {
            private const val SEPARATOR = "-v"
            private const val REGEX_PATTERN = "^(.+)-v(\\d+)$"

            fun of(value: String) = Regex(REGEX_PATTERN).find(value)?.let { match ->
                Id(match.groups[1]!!.value, match.groups[2]!!.value.toInt())
            } ?: throw RuntimeException("Kunne ikke tolke skjemanavn-format, forventet: <skjema-navn>_v<skjema-versjon> og ikke $value")
        }
    }
}
