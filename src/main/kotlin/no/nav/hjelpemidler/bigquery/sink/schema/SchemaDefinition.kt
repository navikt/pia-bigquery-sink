package no.nav.hjelpemidler.bigquery.sink.schema

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo

interface SchemaDefinition {
    val schemaId: Id

    fun entry() = schemaId to this

    fun define(): TableDefinition

    fun transform(payload: JsonNode): RowToInsert

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
            private const val SEPARATOR = "_v"

            fun of(value: String) = value.split(SEPARATOR).let {
                Id(it.first(), it.last().toInt())
            }
        }
    }
}
