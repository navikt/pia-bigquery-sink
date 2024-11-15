package no.nav.pia.bigquery.sink

import com.google.cloud.bigquery.InsertAllRequest
import ia.felles.integrasjoner.kafkameldinger.eksport.InnholdMelding
import ia.felles.integrasjoner.kafkameldinger.eksport.InnholdStatus
import ia.felles.integrasjoner.kafkameldinger.eksport.PlanMelding
import ia.felles.integrasjoner.kafkameldinger.eksport.TemaMelding
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.datetime.LocalDate
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
                                val plan = json.decodeFromString<PlanKafkamelding>(melding.value())
                                log.info("Mottok plan med id: ${plan.id}")
                                bigQueryService.insertPlan(plan = plan)
                            } catch (e: IllegalArgumentException) {
                                log.error(
                                    "Mottok feil formatert kafkamelding i topic: ${topic.navnMedNamespace}, melding: '${melding.value()}'",
                                    e,
                                )
                            }
                        }

                        log.info("Behandlet ${records.count()} meldinger i $consumer (topic '${topic.navnMedNamespace}') ")
                        consumer.commitSync()
                    } catch (e: RetriableException) {
                        log.warn("Had a retriable exception in $consumer (topic '${topic.navnMedNamespace}'), retrying", e)
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
    data class PlanKafkamelding(
        override val id: String,
        override val samarbeidId: Int,
        override val sistEndret: LocalDateTime,
        override val temaer: List<TemaKafkamelding>,
    ) : PlanMelding {
        fun tilRad(): InsertAllRequest.RowToInsert =
            mapOf(
                "id" to id,
                "samarbeidId" to samarbeidId,
                "endret" to sistEndret.toString(),
                "tidsstempel" to "AUTO",
            ).toRowToInsert()
    }

    @Serializable
    data class TemaKafkamelding(
        override val id: Int,
        override val navn: String,
        override val inkludert: Boolean,
        override val innhold: List<InnholdKafkamelding>,
    ) : TemaMelding {
        fun tilRad(planId: String): InsertAllRequest.RowToInsert =
            mapOf(
                "id" to id,
                "navn" to navn,
                "inkludert" to inkludert,
                "planId" to planId,
            ).toRowToInsert()
    }

    @Serializable
    data class InnholdKafkamelding(
        override val id: Int,
        override val navn: String,
        override val inkludert: Boolean,
        override val status: InnholdStatus? = null,
        override val startDato: LocalDate? = null,
        override val sluttDato: LocalDate? = null,
    ) : InnholdMelding {
        fun tilRad(temaId: Int): InsertAllRequest.RowToInsert {
            val obligatoriskeFelter = mapOf(
                "id" to id,
                "temaId" to temaId,
                "navn" to navn,
                "inkludert" to inkludert,
            )

            return if (!inkludert) {
                obligatoriskeFelter.toRowToInsert()
            } else {
                obligatoriskeFelter.plus(
                    mapOf(
                        "status" to status.toString(),
                        "startDato" to startDato.toString(),
                        "sluttdato" to startDato.toString(),
                    ),
                ).toRowToInsert()
            }
        }
    }
}
