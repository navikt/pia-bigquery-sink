package no.nav.pia.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.*
import no.nav.pia.bigquery.sink.helse.Helse
import no.nav.pia.bigquery.sink.helse.Helsesjekk
import no.nav.pia.bigquery.sink.konfigurasjon.Kafka
import no.nav.pia.bigquery.sink.schema.SchemaDefinition
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.coroutines.CoroutineContext

class PiaKafkaLytter : CoroutineScope, Helsesjekk {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private lateinit var job: Job
    private lateinit var konfigurasjon: Kafka
    private lateinit var topic: String
    private lateinit var bigQueryHendelseMottak: BigQueryHendelseMottak

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + job

    init {
        Runtime.getRuntime().addShutdownHook(Thread(this::cancel))
    }

    fun create(topic: String, kafkaKonfigurasjon: Kafka, bigQueryHendelseMottak: BigQueryHendelseMottak) {
        logger.info("Creating kafka consumer job for statistikk")
        this.job = Job()
        this.topic = topic
        this.konfigurasjon = kafkaKonfigurasjon
        this.bigQueryHendelseMottak = bigQueryHendelseMottak
        logger.info("Created kafka consumer job for statistikk")
    }

    fun run() {
        launch {
            KafkaConsumer(
                konfigurasjon.consumerProperties(konfigurasjon.consumerGroup(topic)),
                StringDeserializer(),
                StringDeserializer()
            ).use { consumer ->
                consumer.subscribe(listOf("${konfigurasjon.topicPrefix}.$topic"))
                logger.info("Kafka consumer subscribed to ${konfigurasjon.topicPrefix}.$topic")

                while (job.isActive) {
                    try {
                        val records = consumer.poll(Duration.ofSeconds(1))
                        if (records.count() < 1) continue
                        logger.info("Fant ${records.count()} nye meldinger i topic: $topic")

                        records.forEach {record ->
                            val payload = ObjectMapper().readValue(record.value().replace("\"næringer\"", "\"neringer\""), JsonNode::class.java)
                            bigQueryHendelseMottak.onPacket(SchemaDefinition.Id.of(topic), payload)
                        }
                        logger.info("Lagret ${records.count()} meldinger i topic: $topic")

                        consumer.commitSync()
                    } catch (e: RetriableException) {
                        logger.warn("Had a retriable exception, retrying", e)
                    }
                }
            }
        }
    }

    private fun isRunning(): Boolean {
        logger.trace("Asked if running")
        return job.isActive
    }

    private fun cancel() {
        logger.info("Stopping kafka consumer job for ${konfigurasjon.topicPrefix}.$topic")
        job.cancel()
        logger.info("Stopped kafka consumer job for ${konfigurasjon.topicPrefix}.$topic")
    }

    override fun helse() = if (isRunning()) Helse.UP else Helse.DOWN
}
