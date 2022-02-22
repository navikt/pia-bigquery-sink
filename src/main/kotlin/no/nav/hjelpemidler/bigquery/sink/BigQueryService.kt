package no.nav.hjelpemidler.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.DatasetId
import mu.KotlinLogging
import mu.withLoggingContext
import no.nav.hjelpemidler.bigquery.sink.registry.schemaRegistry
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition

class BigQueryService(
    private val datasetId: DatasetId,
    private val client: BigQueryClient,
) {
    private fun <T> withLoggingContext(block: () -> T) = withLoggingContext(
        "projectId" to datasetId.project,
        "datasetId" to datasetId.dataset
    ) { block() }

    fun ping(): Boolean = withLoggingContext {
        when (client.datasetPresent(datasetId)) {
            false -> log.error { "Fikk ikke kontakt med BigQuery" }.let { false }
            true -> log.info { "Fikk kontakt med BigQuery" }.let { true }
        }
    }

    fun migrate() = withLoggingContext {
        log.info { "Kjører migrering" }
        schemaRegistry
            .mapValues { it.value.toTableInfo(datasetId) }
            .filterValues { !client.tablePresent(it.tableId) }
            .forEach { (_, tableInfo) ->
                client.create(tableInfo)
            }
    }

    fun insert(event: BigQuerySinkEvent) = withLoggingContext {
        val schemaId = event.schemaId
        val schemaDefinition = requireNotNull(schemaRegistry[schemaId]) {
            "Mangler skjema: '$schemaId' i schemaRegistry, følgende skjema finnes: ${schemaRegistry.keys}"
        }
        val tableId = schemaId.toTableId(datasetId)

        client.insert(tableId, schemaDefinition.transform(event.payload))
    }

    companion object {
        private val log = KotlinLogging.logger {}
    }
}

data class BigQuerySinkEvent(
    val schemaId: SchemaDefinition.Id,
    val payload: JsonNode,
)
