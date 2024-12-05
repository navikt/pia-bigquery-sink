package no.nav.pia.bigquery.sink

import com.google.cloud.bigquery.InsertAllRequest
import ia.felles.integrasjoner.kafkameldinger.eksport.SpørreundersøkelseEksportMelding
import ia.felles.integrasjoner.kafkameldinger.spørreundersøkelse.SpørreundersøkelseStatus
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.datetime.LocalDateTime
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import no.nav.pia.bigquery.sink.datadefenisjoner.toRowToInsert
import no.nav.pia.bigquery.sink.helse.Helse
import no.nav.pia.bigquery.sink.helse.Helsesjekk
import no.nav.pia.bigquery.sink.konfigurasjon.KafkaConfig
import no.nav.pia.bigquery.sink.konfigurasjon.KafkaTopic
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.coroutines.CoroutineContext

class SpørreundersøkelseConsumer(
    kafkaConfig: KafkaConfig,
    private val bigQueryService: BigQueryService,
) : CoroutineScope,
    Helsesjekk {
    private val log: Logger = LoggerFactory.getLogger(this::class.java)
    private val job: Job = Job()
    private val topic = KafkaTopic.BEHOVSVURDERING_TOPIC
    private val kafkaConsumer = KafkaConsumer(
        kafkaConfig.consumerProperties(consumerGroupId = topic.konsumentGruppe),
        StringDeserializer(),
        StringDeserializer(),
    )

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + job

    init {
        Runtime.getRuntime().addShutdownHook(Thread(this::cancel))
    }

    private val json = Json {
        ignoreUnknownKeys = true
    }

    fun run() {
        launch {
            kafkaConsumer.use { consumer ->
                consumer.subscribe(listOf(topic.navnMedNamespace))
                log.info(
                    "Kafka consumer subscribed to topic '${topic.navnMedNamespace}' of groupId '${topic.konsumentGruppe}' )' in $consumer",
                )

                while (job.isActive) {
                    try {
                        val records = consumer.poll(Duration.ofSeconds(1))
                        if (records.count() < 1) continue
                        log.info("Fant ${records.count()} nye meldinger i topic: ${topic.navn}")

                        records.forEach { melding ->
                            try {
                                val behovsvurdering = json.decodeFromString<SpørreundersøkelseEksport>(melding.value())
                                log.info("Mottok spørreundersøkelse av typen '${behovsvurdering.type}' med id: ${behovsvurdering.id}")
                                bigQueryService.insert(behovsvurdering = behovsvurdering)
                            } catch (e: IllegalArgumentException) {
                                log.error(
                                    "Mottok feil formatert kafkamelding i topic: ${topic.navnMedNamespace}, melding: '${melding.value()}'",
                                    e,
                                )
                            }
                        }

                        log.info("Behandlet ${records.count()} meldinger i topic '${topic.navn}'")
                        consumer.commitSync()
                    } catch (e: RetriableException) {
                        log.warn("Had a retriable exception in topic '${topic.navnMedNamespace}', retrying", e)
                    } catch (e: Exception) {
                        log.error("Exception is shutting down kafka listner for ${topic.navnMedNamespace}", e)
                        job.cancel(CancellationException(e.message))
                        throw e
                    }
                }
            }
        }
    }

    private fun isRunning(): Boolean {
        log.trace("Asked if running")
        return job.isActive
    }

    private fun cancel() {
        log.info("Stopping kafka consumer job for ${topic.navnMedNamespace}")
        job.cancel()
        log.info("Stopped kafka consumer job for ${topic.navnMedNamespace}")
    }

    override fun helse() = if (isRunning()) Helse.UP else Helse.DOWN

    @Serializable
    data class SpørreundersøkelseEksport(
        override val id: String,
        override val orgnr: String,
        override val type: String,
        override val status: SpørreundersøkelseStatus,
        override val samarbeidId: Int,
        override val saksnummer: String,
        override val opprettetAv: String,
        override val opprettet: LocalDateTime,
        override val harMinstEttSvar: Boolean,
        override val endret: LocalDateTime? = null,
        override val påbegynt: LocalDateTime? = null,
        override val fullført: LocalDateTime? = null,
        override val førsteSvarMotatt: LocalDateTime? = null,
        override val sisteSvarMottatt: LocalDateTime? = null,
    ) : SpørreundersøkelseEksportMelding {
        fun tilRad(): InsertAllRequest.RowToInsert {
            val felter = mutableMapOf(
                "id" to id,
                "orgnr" to orgnr,
                "type" to type,
                "status" to status.toString(),
                "samarbeidId" to samarbeidId,
                "saksnummer" to saksnummer,
                "opprettetAv" to opprettetAv,
                "opprettet" to opprettet.toString(),
                "harMinstEttSvar" to harMinstEttSvar,
                "tidsstempel" to "AUTO",
            )

            // Optional felter
            endret?.let { felter["endret"] = it.toString() }
            påbegynt?.let { felter["pabegynt"] = it.toString() }
            fullført?.let { felter["fullfort"] = it.toString() }
            førsteSvarMotatt?.let { felter["forsteSvarMottatt"] = it.toString() }
            sisteSvarMottatt?.let { felter["sisteSvarMottatt"] = it.toString() }

            return felter.toRowToInsert()
        }
    }
}
