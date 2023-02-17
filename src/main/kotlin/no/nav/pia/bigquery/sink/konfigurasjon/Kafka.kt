package no.nav.pia.bigquery.sink.konfigurasjon

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs

class Kafka(
    val brokers: String = getEnvVar("KAFKA_BROKERS"),
    val truststoreLocation: String = getEnvVar("KAFKA_TRUSTSTORE_PATH"),
    val keystoreLocation: String = getEnvVar("KAFKA_KEYSTORE_PATH"),
    val credstorePassword: String = getEnvVar("KAFKA_CREDSTORE_PASSWORD"),
    val iaSakHendelseTopic: String = getEnvVar("IA_SAK_HENDELSE_TOPIC"),
    val iaSakTopic: String = getEnvVar("IA_SAK_TOPIC"),
    val consumerLoopDelay: Long = getEnvVar("CONSUMER_LOOP_DELAY").toLong()
) {
    companion object {
        const val statistikkConsumerGroupId = "lydia-api-kafka-group-id"
        const val statistikkPerKategoriGroupId = "lydia-api-statistikk-per-kategori-consumer"
        const val brregConsumerGroupId = "lydia-api-brreg-oppdatering-consumer"
        const val clientId: String = "lydia-api"
    }

    fun securityConfigs() =
        mapOf(
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "SSL",
            SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG to "",
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG to "JKS",
            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG to "PKCS12",
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to truststoreLocation,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to credstorePassword,
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to keystoreLocation,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to credstorePassword,
            SslConfigs.SSL_KEY_PASSWORD_CONFIG to credstorePassword
        )

    fun consumerProperties(consumerGroupId: String = statistikkConsumerGroupId) =
        baseConsumerProperties(consumerGroupId).apply {
            // TODO: Finn smidigere måte å få tester til å kjøre
            if (truststoreLocation.isBlank()) {
                put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT")
                put(SaslConfigs.SASL_MECHANISM, "PLAIN")
            } else {
                putAll(securityConfigs())
            }
        }

    private fun baseConsumerProperties(consumerGroupId: String) =
        mapOf(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to brokers,
            ConsumerConfig.GROUP_ID_CONFIG to consumerGroupId,
            ConsumerConfig.CLIENT_ID_CONFIG to clientId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "1000"
        ).toProperties()

}

