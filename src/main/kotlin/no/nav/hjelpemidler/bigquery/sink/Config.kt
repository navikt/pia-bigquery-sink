package no.nav.hjelpemidler.bigquery.sink

import com.natpryce.konfig.Configuration
import com.natpryce.konfig.ConfigurationMap
import com.natpryce.konfig.ConfigurationProperties
import com.natpryce.konfig.EnvironmentVariables
import com.natpryce.konfig.Key
import com.natpryce.konfig.overriding
import com.natpryce.konfig.stringType
import kotlin.reflect.KProperty

enum class Environment {
    LOCAL, DEV, PROD;

    fun pair(): Pair<String, String> = Pair(KEY.name, name)

    internal companion object {
        private val KEY = Key("ENVIRONMENT", stringType)

        internal fun from(config: Config): Environment = valueOf(config[KEY])
    }
}

private class EnvironmentVariable {
    operator fun getValue(thisRef: Any?, property: KProperty<*>): String = when (thisRef) {
        null -> property.name.uppercase()
        else -> "${thisRef::class.simpleName}_${property.name}".uppercase()
    }
}

private val envVar = EnvironmentVariable()
private val http_port by envVar
private val rapid_app_name by envVar

object Nais {
    val cluster_name by envVar
}

object Gcp {
    val team_project_id by envVar
}

object BigQuery {
    val dataset_id by envVar
}

object Kafka {
    val brokers by envVar
    val consumer_group_id by envVar
    val rapid_topic by envVar
    val keystore_path by envVar
    val truststore_path by envVar
    val credstore_password by envVar
    val reset_policy by envVar
}

object Config {
    private val defaultProperties = ConfigurationMap(
        Environment.LOCAL.pair(),

        http_port to "7073",
        rapid_app_name to "hm-bigquery-sink",

        Gcp.team_project_id to "teamdigihot",

        Kafka.brokers to "host.docker.internal:9092",
        Kafka.consumer_group_id to "hm-bigquery-sink-v1",
        Kafka.rapid_topic to "teamdigihot.hm-soknadsbehandling-v1",
        Kafka.keystore_path to "",
        Kafka.truststore_path to "",
        Kafka.credstore_password to "",
        Kafka.reset_policy to "earliest",

        BigQuery.dataset_id to "hm_bigquery_sink_v1_dataset_local"
    )

    private val devProperties = ConfigurationMap(
        Environment.DEV.pair(),

        http_port to "8080",

        BigQuery.dataset_id to "hm_bigquery_sink_v1_dataset_dev"
    )

    private val prodProperties = ConfigurationMap(
        Environment.PROD.pair(),

        http_port to "8080",

        BigQuery.dataset_id to "hm_bigquery_sink_v1_dataset_prod"
    )

    private val properties: Configuration by lazy {
        val systemAndEnvProperties = ConfigurationProperties.systemProperties() overriding EnvironmentVariables()
        when (System.getenv().getOrDefault(Nais.cluster_name, "local").lowercase()) {
            "dev-gcp" -> systemAndEnvProperties overriding devProperties overriding defaultProperties
            "prod-gcp" -> systemAndEnvProperties overriding prodProperties overriding defaultProperties
            else -> systemAndEnvProperties overriding defaultProperties
        }
    }

    val environment: Environment get() = Environment.from(this)

    operator fun <T> get(key: Key<T>): T = properties[key]
    operator fun get(key: String): String = get(Key(key, stringType))

    fun asMap(): Map<String, String> = properties.list().reversed().fold(emptyMap()) { map, pair ->
        map + pair.second
    }
}
