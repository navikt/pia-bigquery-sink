package no.nav.pia.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import no.nav.pia.bigquery.sink.datadefenisjoner.schemaRegistry
import no.nav.pia.bigquery.sink.konfigurasjon.Clusters
import no.nav.pia.bigquery.sink.konfigurasjon.NaisEnvironment
import no.nav.pia.bigquery.sink.schema.Registry
import no.nav.pia.bigquery.sink.schema.SchemaDefinition
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class BigQueryService(
    private val client: BigQueryClient,
) {
    fun migrate(registry: Registry) {
        log.info("Kjører migrering")
        val tableInfoById = registry.mapValues { it.value.toTableInfo(registry.datasetId) }

        // create missing tables
        tableInfoById
            .filterValues { !client.tablePresent(it.tableId) }
            .forEach { (_, tableInfo) -> client.create(tableInfo) }
        // add missing columns
        tableInfoById
            .filterValues { client.tablePresent(it.tableId) }
            .forEach { (_, tableInfo) -> client.update(tableInfo.tableId, tableInfo) }
    }

    fun insert(
        registry: Registry,
        schemaId: SchemaDefinition.Id,
        payload: JsonNode,
    ) {
        val schemaDefinition = requireNotNull(registry[schemaId]) {
            "Mangler skjema: '$schemaId' i schemaRegistry, følgende skjema finnes: ${schemaRegistry.keys}"
        }
        val tableId = schemaId.toTableId(registry.datasetId)

        if (NaisEnvironment.cluster == Clusters.DEV_GCP.clusterId) {
            log.info("payload: '$payload'")
        }

        if (schemaDefinition.skip(payload)) {
            if (NaisEnvironment.cluster == Clusters.DEV_GCP.clusterId) {
                log.info("skip: true, payload: '$payload'")
            }
            return
        }

        runCatching {
            client.insert(tableId, schemaDefinition.transform(payload))
        }.onFailure { exception ->

            log.error("insert feilet: ${exception.message}")
            throw exception
        }
    }

    companion object {
        val log: Logger = LoggerFactory.getLogger(BigQueryService::class.java)
    }
}
