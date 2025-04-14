package no.nav.pia.bigquery.sink

import com.google.cloud.bigquery.InsertAllRequest
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

class SamarbeidConsumer(
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
                                val samarbeid = json.decodeFromString<SamarbeidEksport>(melding.value())
                                bigQueryService.insert(samarbeid = samarbeid)
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
    data class SamarbeidEksport(
        val id: Int,
        val saksnummer: String,
        val opprettet: LocalDateTime,
        val navn: String? = null,
        val status: String? = null,
        val avbrutt: LocalDateTime? = null,
        val fullført: LocalDateTime? = null,
        val sistEndret: LocalDateTime? = null,
    ) {
        fun tilRad(): InsertAllRequest.RowToInsert {
            // Required
            val felter = mutableMapOf(
                "id" to id,
                "saksnummer" to saksnummer,
                "opprettet" to opprettet.toString(),
                "tidsstempel" to "AUTO",
            )

            // Optional
            navn?.let { felter["navn"] = it }
            status?.let { felter["status"] = it }
            avbrutt?.let { felter["avbrutt"] = it.toString() }
            fullført?.let { felter["fullfort"] = it.toString() }
            sistEndret?.let { felter["endret"] = it.toString() }

            return felter.toRowToInsert()
        }
    }
}
