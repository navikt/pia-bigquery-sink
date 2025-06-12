package no.nav.pia.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.TableId
import no.nav.pia.bigquery.sink.SamarbeidConsumer.SamarbeidMelding
import no.nav.pia.bigquery.sink.SamarbeidsplanConsumer.InnholdIPlanMelding
import no.nav.pia.bigquery.sink.SpørreundersøkelseConsumer.SpørreundersøkelseEksport
import no.nav.pia.bigquery.sink.datadefenisjoner.DATASET_ID
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`samarbeid-v1`
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`samarbeidsplan-v1`
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`sporreundersokelse-v1`
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
    companion object {
        val log: Logger = LoggerFactory.getLogger(BigQueryService::class.java)
    }

    fun migrate(registry: Registry) {
        log.info("Kjører migrering")
        val tableInfoById = registry.mapValues { it.value.toTableInfo(datasetId = registry.datasetId) }

        // create missing tables
        tableInfoById
            .filterValues { !client.tablePresent(tableId = it.tableId) }
            .forEach { (_, tableInfo) -> client.create(tableInfo) }
        // add missing columns
        tableInfoById
            .filterValues { client.tablePresent(tableId = it.tableId) }
            .forEach { (_, tableInfo) -> client.update(tableId = tableInfo.tableId, updatedTableInfo = tableInfo) }
    }

    fun insert(
        registry: Registry,
        schemaId: SchemaDefinition.Id,
        payload: JsonNode,
    ) {
        val schemaDefinition = requireNotNull(registry[schemaId]) {
            "Mangler skjema: '$schemaId' i schemaRegistry, følgende skjema finnes: ${schemaRegistry.keys}"
        }
        val tableId = schemaId.toTableId(datasetId = registry.datasetId)

        if (schemaDefinition.skip(payload)) {
            if (NaisEnvironment.cluster == Clusters.DEV_GCP.clusterId) {
                log.info("skip: true, payload: '$payload'")
            }
            return
        }

        runCatching {
            client.insert(tableId, row = schemaDefinition.transform(payload))
        }.onFailure { exception ->

            log.error("insert feilet: ${exception.message}")
            throw exception
        }
    }

    fun insert(undertemaer: List<InnholdIPlanMelding>) {
        val samarbeidsplanTableId = TableId.of(
            DATASET_ID.project,
            DATASET_ID.dataset,
            `samarbeidsplan-v1`.schemaId.toTableName(),
        )

        undertemaer.forEach { undertema ->
            runCatching {
                client.insert(tableId = samarbeidsplanTableId, row = undertema.tilRad())
                log.debug("Samarbeidsplan lagret i Bigquery")
            }.onFailure { exception ->
                log.error(
                    "insert feilet for undertema: '${undertema.id}' knyttet til tema: '${undertema.temaId}', knyttet til plan: '${undertema.planId}' - feilmelding: ${exception.message}",
                )
                throw exception
            }
        }
    }

    fun insert(behovsvurdering: SpørreundersøkelseEksport) {
        val tableId = TableId.of(DATASET_ID.project, DATASET_ID.dataset, `sporreundersokelse-v1`.schemaId.toTableName())

        runCatching {
            client.insert(tableId = tableId, row = behovsvurdering.tilRad())
        }.onFailure { exception ->
            log.error(
                "insert feilet for behovsvurdering '${behovsvurdering.id}' og for samarbeid '${behovsvurdering.samarbeidId}' - feilmelding: ${exception.message}",
            )
            throw exception
        }
    }

    fun insert(samarbeid: SamarbeidMelding) {
        val tableId = TableId.of(DATASET_ID.project, DATASET_ID.dataset, `samarbeid-v1`.schemaId.toTableName())

        runCatching {
            client.insert(tableId = tableId, row = samarbeid.tilRad())
        }.onFailure { exception ->
            log.error("insert feilet for samarbeid '${samarbeid.id}' feilmelding: ${exception.message}")
            throw exception
        }
    }
}
