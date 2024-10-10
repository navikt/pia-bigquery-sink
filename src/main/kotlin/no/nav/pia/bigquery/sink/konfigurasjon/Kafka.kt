package no.nav.pia.bigquery.sink.konfigurasjon

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs

class Kafka(
    private val brokers: String = getEnvVar("KAFKA_BROKERS"),
    private val truststoreLocation: String = getEnvVar("KAFKA_TRUSTSTORE_PATH"),
    private val keystoreLocation: String = getEnvVar("KAFKA_KEYSTORE_PATH"),
    private val credstorePassword: String = getEnvVar("KAFKA_CREDSTORE_PASSWORD"),
    val topicPrefix: String = getEnvVar("KAFKA_TOPIC_PREFIX", "pia"),
    val iaSakStatistikkTopic: String = getEnvVar("IA_SAK_STATISTIKK_TOPIC"),
    val iaSakLeveranseTopic: String = getEnvVar("IA_SAK_LEVERANSE_TOPIC"),
    val behovsvurderingTopic: String = getEnvVar("BEHOVSVURDERING_TOPIC"),
) {
    companion object {
        const val CLIENT_ID: String = "pia-bigquery-sink"
        const val IA_SAK_STATISTIKK_CONSUMER_GROUP_ID = "ia-sak-statistikk_$CLIENT_ID"
        const val IA_SAK_LEVERANSE_CONSUMER_GROUP_ID = "ia-sak-leveranse_$CLIENT_ID"
        const val BEHOVSVURDERING_CONSUMER_GROUP_ID = "behovsvurdering-bigquery_$CLIENT_ID"
    }

    fun consumerGroup(topic: String) =
        when (topic) {
            iaSakStatistikkTopic -> IA_SAK_STATISTIKK_CONSUMER_GROUP_ID
            iaSakLeveranseTopic -> IA_SAK_LEVERANSE_CONSUMER_GROUP_ID
            behovsvurderingTopic -> BEHOVSVURDERING_CONSUMER_GROUP_ID
            else -> throw IllegalStateException("Ukjent topic. Aner ikke hvilken consumergroup som skal benyttes")
        }

    private fun securityConfigs() =
        mapOf(
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "SSL",
            SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG to "",
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG to "JKS",
            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG to "PKCS12",
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to truststoreLocation,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to credstorePassword,
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to keystoreLocation,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to credstorePassword,
            SslConfigs.SSL_KEY_PASSWORD_CONFIG to credstorePassword,
        )

    fun consumerProperties(consumerGroupId: String) =
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
            ConsumerConfig.CLIENT_ID_CONFIG to CLIENT_ID,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "1000",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
        ).toProperties()
}
