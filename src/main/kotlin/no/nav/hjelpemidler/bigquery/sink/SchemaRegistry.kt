package no.nav.hjelpemidler.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo

data class SchemaId(val name: String, val version: Int) {
    fun toTableId(datasetId: DatasetId): TableId = TableId.of(
        datasetId.project,
        datasetId.dataset,
        "${name}_v${version}"
    )
}

interface SchemaDefinition {
    val schemaId: SchemaId

    fun define(): Schema

    fun transform(payload: JsonNode): RowToInsert

    fun toTableInfo(datasetId: DatasetId): TableInfo {
        val tableDefinition = StandardTableDefinition.of(define())
        return TableInfo.of(schemaId.toTableId(datasetId), tableDefinition)
    }
}

class SchemaRegistry(
    private val schemas: Map<SchemaId, SchemaDefinition>,
) : Map<SchemaId, SchemaDefinition> by schemas {

    companion object {
        private val schemas: List<SchemaDefinition> = listOf(
            HendelseSchema,
        )

        fun create(): SchemaRegistry = SchemaRegistry(schemas.associateBy { it.schemaId })
    }
}
