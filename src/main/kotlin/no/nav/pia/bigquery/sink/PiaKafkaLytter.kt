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

object PiaKafkaLytter : CoroutineScope, Helsesjekk {
    private val logger: Logger = LoggerFactory.getLogger(this::class.java)
    private lateinit var job: Job
    private lateinit var konfigurasjon: Kafka
    private lateinit var bigQueryHendelseMottak: BigQueryHendelseMottak

    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + job

    init {
        Runtime.getRuntime().addShutdownHook(Thread(PiaKafkaLytter::cancel))
    }

    fun create(kafkaKonfigurasjon: Kafka, bigQueryHendelseMottak: BigQueryHendelseMottak) {
        logger.info("Creating kafka consumer job for statistikk")
        this.job = Job()
        this.konfigurasjon = kafkaKonfigurasjon
        this.bigQueryHendelseMottak = bigQueryHendelseMottak
        logger.info("Created kafka consumer job for statistikk")
    }

    fun run() {
        launch {
            KafkaConsumer(
                konfigurasjon.consumerProperties(),
                StringDeserializer(),
                StringDeserializer()
            ).use { consumer ->
                consumer.subscribe(listOf("${konfigurasjon.topicPrefix}.${konfigurasjon.iaSakHendelseTopic}"))
                logger.info("Kafka consumer subscribed to ${konfigurasjon.topicPrefix}.${konfigurasjon.iaSakHendelseTopic}")

                while (job.isActive) {
                    try {
                        val records = consumer.poll(Duration.ofSeconds(1))
                        if (records.count() < 1) continue
                        logger.info("Fant ${records.count()} nye meldinger")

                        records.forEach {record ->
                            val payload = ObjectMapper().readValue(record.value(), JsonNode::class.java)
                            bigQueryHendelseMottak.onPacket(SchemaDefinition.Id.of(konfigurasjon.iaSakHendelseTopic), payload)
                        }
                        logger.info("Lagret ${records.count()} meldinger")

                        consumer.commitSync()
                    } catch (e: RetriableException) {
                        logger.warn("Had a retriable exception, retrying", e)
                    }
                    delay(konfigurasjon.consumerLoopDelay)
                }

            }
        }
    }

    private fun isRunning(): Boolean {
        logger.trace("Asked if running")
        return job.isActive
    }

    private fun cancel() {
        logger.info("Stopping kafka consumer job for ${konfigurasjon.topicPrefix}.${this.konfigurasjon.iaSakHendelseTopic}")
        job.cancel()
        logger.info("Stopped kafka consumer job for ${konfigurasjon.topicPrefix}.${this.konfigurasjon.iaSakHendelseTopic}")
    }

    override fun helse() = if (isRunning()) Helse.UP else Helse.DOWN
}
