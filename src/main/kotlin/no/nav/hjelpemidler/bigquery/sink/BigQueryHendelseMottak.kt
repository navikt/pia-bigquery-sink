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
    private val bigQueryClient: BigQueryClient,
) : PacketListener {
    private companion object {
        private val log = KotlinLogging.logger {}
    }

    init {
        River(rapidsConnection).apply {
            validate { it.demandValue("eventName", "hm-bigquery-hendelse") }
            validate { it.requireKey("destination", "payload") }
            validate { it.interestedIn("created") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val destination = packet["destination"].asText()
        val created = packet["created"].asText()
        val payload = packet["payload"]

        withLoggingContext(
            "destination" to destination,
            "created" to created,
        ) {
            log.info { payload.toString() }
        }
    }
}
