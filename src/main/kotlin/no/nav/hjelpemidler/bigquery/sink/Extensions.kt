package no.nav.hjelpemidler.bigquery.sink

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

/**
 * Produce value suitable for insertion in BigQuery.
 */
fun LocalDateTime.toDateTime(): String = truncatedTo(ChronoUnit.MICROS).toString()
