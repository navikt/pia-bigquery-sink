package no.nav.pia.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import no.nav.pia.bigquery.sink.helse.Helse
import no.nav.pia.bigquery.sink.helse.Helsesjekk
import no.nav.pia.bigquery.sink.konfigurasjon.KafkaConfig
import no.nav.pia.bigquery.sink.konfigurasjon.KafkaTopic
import no.nav.pia.bigquery.sink.schema.SchemaDefinition
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.coroutines.CoroutineContext

class PiaKafkaLytter(
    kafkaConfig: KafkaConfig,
    private val bigQueryHendelseMottak: BigQueryHendelseMottak,
    private val topic: KafkaTopic,
) : CoroutineScope,
    Helsesjekk {
    private val log: Logger = LoggerFactory.getLogger(this::class.java)
    private val job: Job = Job()

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

    fun run() {
        launch {
            kafkaConsumer.use { consumer ->
                consumer.subscribe(listOf(topic.navnMedNamespace))
                log.info("Kafka consumer subscribed to ${topic.navnMedNamespace}")

                while (job.isActive) {
                    try {
                        val records = consumer.poll(Duration.ofSeconds(1))
                        if (records.count() < 1) continue
                        log.info("Fant ${records.count()} nye meldinger i topic: $topic")

                        records.forEach { record ->
                            val payload = ObjectMapper().readValue(
                                record.value().replace("\"næringer\"", "\"neringer\""),
                                JsonNode::class.java,
                            )
                            bigQueryHendelseMottak.onPacket(SchemaDefinition.Id.of(topic.navn), payload)
                        }
                        log.info("Lagret ${records.count()} meldinger i topic: ${topic.navn}")
                        consumer.commitSync()
                    } catch (e: RetriableException) {
                        log.warn("Had a retriable exception, retrying", e)
                    } catch (e: Exception) {
                        log.error("Exception is shutting down kafka listner for ${topic.navn}", e)
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
}
