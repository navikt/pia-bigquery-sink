package no.nav.hjelpemidler.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import mu.KotlinLogging
import mu.withLoggingContext
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.River.PacketListener
import no.nav.hjelpemidler.bigquery.sink.brille.brilleRegistry
import no.nav.hjelpemidler.bigquery.sink.registry.hendelse_v2
import no.nav.hjelpemidler.bigquery.sink.registry.schemaRegistry
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import java.time.LocalDate
import java.time.Month

class BigQueryHendelseMottak(
    rapidsConnection: RapidsConnection,
    private val bigQueryService: BigQueryService,
) : PacketListener {
    private companion object {
        private val log = KotlinLogging.logger {}
    }

    init {
        River(rapidsConnection).apply {
            validate { it.demandValue("eventName", "hm-bigquery-sink-hendelse") }
            validate { it.requireKey("schemaId", "payload") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val schemaId = packet["schemaId"].asSchemaId()
        val payload = packet["payload"]

        if (skip(schemaId, payload)) {
            withLoggingContext(
                "schemaId" to schemaId.toString(),
                "payload" to payload.toString()
            ) {
                log.info { "Hopper over melding" }
            }
            return
        }

        withLoggingContext(
            "schemaName" to schemaId.name,
            "schemaVersion" to schemaId.version.toString(),
        ) {
            log.debug { "Mottok hendelse for lagring i BigQuery" }
            val registry = if (schemaRegistry.containsKey(schemaId)) {
                schemaRegistry
            } else if (brilleRegistry.containsKey(schemaId)) {
                brilleRegistry
            } else {
                error("Fant ikke register for tabell: $schemaId")
            }
            bigQueryService.insert(registry, BigQuerySinkEvent(schemaId, payload))
        }
    }

    private fun skip(schemaId: SchemaDefinition.Id, payload: JsonNode): Boolean = when (schemaId) {
        hendelse_v2.schemaId -> {
            val opprettet = payload["opprettet"].asLocalDateTime()
            val navn = payload["navn"].asText()
            when {
                navn == "hm-bestillingsordning-river.requestFeilet"
                        && opprettet.isBefore(LocalDate.of(2022, Month.MAY, 4).atStartOfDay()) -> true
                else -> false
            }
        }
        else -> false
    }
}
