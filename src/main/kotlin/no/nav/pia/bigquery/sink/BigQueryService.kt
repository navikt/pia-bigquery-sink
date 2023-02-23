package no.nav.pia.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import mu.KotlinLogging
import mu.withLoggingContext
import no.nav.pia.bigquery.sink.datadefenisjoner.schemaRegistry
import no.nav.pia.bigquery.sink.konfigurasjon.Clusters
import no.nav.pia.bigquery.sink.konfigurasjon.Miljø
import no.nav.pia.bigquery.sink.schema.Registry
import no.nav.pia.bigquery.sink.schema.SchemaDefinition

class BigQueryService(
    private val projectId: String,
    private val client: BigQueryClient,
) {
    private fun <T> withLoggingContext(block: () -> T) = withLoggingContext(
        "projectId" to projectId,
    ) { block() }

    fun migrate(registry: Registry) = withLoggingContext {
        log.info { "Kjører migrering" }
        val tableInfoById = registry.mapValues {
            it.value.toTableInfo(registry.datasetId)
        }

        // create missing tables
        tableInfoById
            .filterValues { !client.tablePresent(it.tableId) }
            .forEach { (_, tableInfo) ->
                client.create(tableInfo)
            }
        // add missing columns
        tableInfoById
            .filterValues { client.tablePresent(it.tableId) }
            .forEach { (_, tableInfo) ->
                client.update(tableInfo.tableId, tableInfo)
            }
    }

    fun insert(registry: Registry, event: BigQuerySinkEvent) = withLoggingContext {
        val schemaId = event.schemaId
        val schemaDefinition = requireNotNull(registry[schemaId]) {
            "Mangler skjema: '$schemaId' i schemaRegistry, følgende skjema finnes: ${schemaRegistry.keys}"
        }
        val tableId = schemaId.toTableId(registry.datasetId)

        if (Miljø.cluster == Clusters.DEV_GCP.clusterId) log.info {
            "payload: '${event.payload}'"
        }

        if (schemaDefinition.skip(event.payload)) {
            log.info { "skip: true, payload: '${event.payload}'" }
            return@withLoggingContext
        }

        runCatching {
            client.insert(tableId, schemaDefinition.transform(event.payload))
        }.onFailure { exception ->
            withLoggingContext(
                "schemaId" to schemaId.toString(),
                "payload" to event.payload.toString(),
            ) {
                log.error(exception) { "insert feilet" }
            }
            throw exception
        }
    }

    companion object {
        private val log = KotlinLogging.logger {}
    }
}

data class BigQuerySinkEvent(
    val schemaId: SchemaDefinition.Id,
    val payload: JsonNode,
)
