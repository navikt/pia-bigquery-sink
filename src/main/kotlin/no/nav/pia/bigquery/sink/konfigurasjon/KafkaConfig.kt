package no.nav.pia.bigquery.sink.konfigurasjon

import no.nav.pia.bigquery.sink.konfigurasjon.KafkaConfig.Companion.CLIENT_ID
import no.nav.pia.bigquery.sink.konfigurasjon.KafkaTopic.IA_SAK_LEVERANSE_TOPIC
import no.nav.pia.bigquery.sink.konfigurasjon.KafkaTopic.IA_SAK_STATISTIKK_TOPIC
import no.nav.pia.bigquery.sink.konfigurasjon.KafkaTopic.SAMARBEID_TOPIC
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs

class KafkaConfig(
    private val brokers: String = getEnvVar("KAFKA_BROKERS"),
    private val truststoreLocation: String = getEnvVar("KAFKA_TRUSTSTORE_PATH"),
    private val keystoreLocation: String = getEnvVar("KAFKA_KEYSTORE_PATH"),
    private val credstorePassword: String = getEnvVar("KAFKA_CREDSTORE_PASSWORD"),
) {
    companion object {
        const val CLIENT_ID: String = "pia-bigquery-sink"
    }

    val generelleTopics = listOf(
        IA_SAK_STATISTIKK_TOPIC,
        IA_SAK_LEVERANSE_TOPIC,
        SAMARBEID_TOPIC,
    )

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
            ConsumerConfig.CLIENT_ID_CONFIG to consumerGroupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to "50",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
        ).toProperties()
}

enum class KafkaTopic(
    val navn: String,
    private val prefix: String = "pia",
) {
    IA_SAK_STATISTIKK_TOPIC("ia-sak-statistikk-v1"),
    IA_SAK_LEVERANSE_TOPIC("ia-sak-leveranse-v1"),
    BEHOVSVURDERING_TOPIC("behovsvurdering-bigquery-v1"),
    SAMARBEID_TOPIC("samarbeid-bigquery-v1"),
    SAMARBEIDSPLAN_TOPIC("samarbeidsplan-bigquery-v1"), ;

    val konsumentGruppe
        get() = "${navn}_$CLIENT_ID"

    val navnMedNamespace
        get() = "$prefix.$navn"
}
