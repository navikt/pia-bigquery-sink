package no.nav.pia.bigquery.sink.konfigurasjon

fun getEnvVar(
    varName: String,
    defaultValue: String? = null,
) = System.getenv(varName) ?: defaultValue ?: throw RuntimeException("Missing required variable $varName")
