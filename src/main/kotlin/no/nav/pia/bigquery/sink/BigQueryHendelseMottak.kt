package no.nav.pia.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import mu.KotlinLogging
import mu.withLoggingContext
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
            withLoggingContext(
                "schemaId" to schemaId.toString(),
            ) {
                log.info { "Hopper over melding for skjemaId: $schemaId" }
            }
            return
        }

        withLoggingContext(
            "schemaName" to schemaId.name,
            "schemaVersion" to schemaId.version.toString(),
        ) {
            log.debug { "Mottok hendelse for lagring i BigQuery" }
            val registry = when {
                schemaRegistry.containsKey(schemaId) -> schemaRegistry
                else -> error("Fant ikke register for tabell: $schemaId")
            }
            bigQueryService.insert(registry, BigQuerySinkEvent(schemaId, payload))
        }
    }

    private fun skip(schemaId: SchemaDefinition.Id): Boolean =
        when (schemaId) {
            SchemaDefinition.Id("skipskjemaid", 1) -> true
            else -> false
        }
}
