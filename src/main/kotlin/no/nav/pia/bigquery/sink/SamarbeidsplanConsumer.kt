package no.nav.pia.bigquery.sink

import com.google.cloud.bigquery.InsertAllRequest
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import no.nav.pia.bigquery.sink.BigQueryService.Companion.log
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

class SamarbeidsplanConsumer(
    kafkaConfig: KafkaConfig,
    private val bigQueryService: BigQueryService,
) : CoroutineScope,
    Helsesjekk {
    private val log: Logger = LoggerFactory.getLogger(this::class.java)
    private val job: Job = Job()
    private val topic = KafkaTopic.SAMARBEIDSPLAN_TOPIC
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
                                val undertemaer = json.decodeFromString<List<InnholdIPlanMelding>>(melding.value())

                                bigQueryService.insert(undertemaer = undertemaer)
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
    data class InnholdIPlanMelding(
        val id: Int,
        val temaId: Int,
        val planId: String,
        val samarbeidId: Int,
        val navn: String,
        val temanavn: String,
        val inkludert: Boolean,
        val sistEndretTidspunktPlan: LocalDateTime,
        val status: Status? = null,
        val startDato: LocalDate? = null,
        val sluttDato: LocalDate? = null,
    ) {
        @Serializable
        enum class Status {
            PLANLAGT,
            PÅGÅR,
            FULLFØRT,
            AVBRUTT,
        }

        fun tilRad(): InsertAllRequest.RowToInsert {
            val obligatoriskeFelter = mapOf(
                "id" to id,
                "tema_id" to temaId,
                "plan_id" to planId,
                "samarbeid_id" to samarbeidId,
                "navn" to navn,
                "temanavn" to temanavn,
                "inkludert" to inkludert,
                "sist_endret_tidspunkt_plan" to sistEndretTidspunktPlan.toString(),
                "tidsstempel" to "AUTO",
            )

            return if (inkludert && status != null && startDato != null && sluttDato != null) {
                obligatoriskeFelter.plus(
                    mapOf(
                        "status" to status.toString(),
                        "startDato" to startDato.toString(),
                        "sluttdato" to startDato.toString(),
                    ),
                ).toRowToInsert()
            } else if (inkludert) {
                log.warn(
                    "Innhold er inkludert for id=$id i tema=$temaId, men felter er ikke rett status='$status', startDato='$startDato', sluttDato'$sluttDato'. Hopper over melding..",
                )
                obligatoriskeFelter.toRowToInsert()
            } else {
                obligatoriskeFelter.toRowToInsert()
            }
        }
    }
}
