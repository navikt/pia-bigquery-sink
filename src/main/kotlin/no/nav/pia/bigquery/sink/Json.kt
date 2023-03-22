package no.nav.pia.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import kotlin.math.roundToInt

private fun JsonNode.asLocalDateTime(): LocalDateTime? = asText().let {
    if (it == "null") return null
    LocalDateTime.parse(it)
}

fun LocalDateTime.oversettFraCetTilUtc() =
    atZone(ZoneId.of("Europe/Oslo"))
        .withZoneSameInstant(ZoneId.of("UTC"))
        .toLocalDateTime()

fun JsonNode.asLocalDate(): String? = asText().let {
    if (it == "null") return null
    LocalDate.parse(it).toString()
}

fun JsonNode.asUtcDateTime(): String? = asLocalDateTime()?.oversettFraCetTilUtc()?.truncatedTo(ChronoUnit.MICROS)?.toString()

fun JsonNode.asBigDecimal(): BigDecimal? =
    asText()
    ?.let { if (it == "null") null else it }
    ?.let { ((it.toDouble() * 1000000).roundToInt() / 1000000.0).toBigDecimal() }

fun <T> JsonNode.use(key: String, transform: JsonNode.() -> T): Pair<String, T?> = key to get(key)?.let {
    transform(it)
}
