package no.nav.pia.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.TableId
import no.nav.pia.bigquery.sink.BehovsvurderingConsumer.BehovsvurderingKafkamelding
import no.nav.pia.bigquery.sink.SamarbeidsplanConsumer.PlanKafkamelding
import no.nav.pia.bigquery.sink.datadefenisjoner.DATASET_ID
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

    fun insertPlan(plan: PlanKafkamelding) {
        val planTableId = TableId.of(DATASET_ID.project, DATASET_ID.dataset, "samarbeidsplan-bigquery-v1")
        val temaTableId = TableId.of(DATASET_ID.project, DATASET_ID.dataset, "samarbeidsplan-tema-bigquery-v1")
        val innholdTableId = TableId.of(DATASET_ID.project, DATASET_ID.dataset, "samarbeidsplan-innhold-bigquery-v1")

        runCatching {
            client.insert(planTableId, plan.tilRad())
            plan.temaer.forEach { tema ->
                client.insert(temaTableId, tema.tilRad(planId = plan.id))
                tema.innhold.forEach { innhold ->
                    client.insert(innholdTableId, innhold.tilRad(temaId = tema.id))
                }
            }
        }.onFailure { exception ->
            log.error(
                "insert feilet for planID '${plan.id}' og samarbeid '${plan.samarbeidId}' - feilmelding: ${exception.message} - plan: $plan",
            )
            throw exception
        }
    }

    fun insertBehovsvurdering(behovsvurdering: BehovsvurderingKafkamelding) {
        val tableId = TableId.of(DATASET_ID.project, DATASET_ID.dataset, "behovsvurdering-bigquery-v1")

        runCatching {
            client.insert(tableId = tableId, behovsvurdering.tilRad())
        }.onFailure { exception ->
            log.error(
                "insert feilet for behovsvurdering '${behovsvurdering.id}' og for samarbeid '${behovsvurdering.samarbeidId}' - feilmelding: ${exception.message} - behovsvurdering: $behovsvurdering",
            )
            throw exception
        }
    }
}
