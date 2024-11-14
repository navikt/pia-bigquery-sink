package no.nav.pia.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import mu.KotlinLogging
import no.nav.pia.bigquery.sink.datadefenisjoner.schemaRegistry
import no.nav.pia.bigquery.sink.schema.SchemaDefinition

class BigQueryHendelseMottak(
    private val bigQueryService: BigQueryService,
) {
    private companion object {
        private val log = KotlinLogging.logger {}
    }

    fun onPacket(
        schemaId: SchemaDefinition.Id,
        payload: JsonNode,
    ) {
        if (skip(schemaId)) {
            log.info { "Hopper over melding for skjemaId: $schemaId" }
            return
        }

        log.info { "Mottok hendelse for lagring i BigQuery" }

        val registry = when {
            schemaRegistry.containsKey(schemaId) -> schemaRegistry
            else -> error("Fant ikke register for tabell: $schemaId")
        }

        bigQueryService.insert(
            registry = registry,
            schemaId = schemaId,
            payload = payload,
        )
    }

    private fun skip(schemaId: SchemaDefinition.Id): Boolean =
        when (schemaId) {
            SchemaDefinition.Id("skipskjemaid", 1) -> true
            else -> false
        }
}
