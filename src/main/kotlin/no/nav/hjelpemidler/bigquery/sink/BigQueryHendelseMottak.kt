package no.nav.hjelpemidler.bigquery.sink

import mu.KotlinLogging
import mu.withLoggingContext
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.River.PacketListener

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

        withLoggingContext(
            "schemaName" to schemaId.name,
            "schemaVersion" to schemaId.version.toString(),
        ) {
            log.info { "Mottok hendelse for lagring i BigQuery" }
            bigQueryService.insert(BigQuerySinkEvent(schemaId, payload))
        }
    }
}
