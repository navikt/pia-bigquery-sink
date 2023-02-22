package no.nav.pia.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

fun JsonNode.asLocalDateTime(): LocalDateTime = LocalDateTime.parse(asText())
fun JsonNode.asDateTime(): String = asLocalDateTime().truncatedTo(ChronoUnit.MICROS).toString()
fun JsonNode.asZonedDateTime(): ZonedDateTime = ZonedDateTime.parse(asText())
fun JsonNode.asTimestamp(): String = asZonedDateTime().truncatedTo(ChronoUnit.MICROS).toInstant().toString()

fun <T> JsonNode.use(key: String, transform: JsonNode.() -> T): Pair<String, T?> = key to get(key)?.let {
    transform(it)
}

infix fun JsonNode?.asTextWithName(key: String): Pair<String, String?> = key to this?.asText()
infix fun JsonNode?.textValueWithName(key: String): Pair<String, String?> = key to this?.textValue()
infix fun JsonNode?.intValueWithName(key: String): Pair<String, Int?> = key to this?.intValue()
infix fun JsonNode?.asBooleanWithName(key: String): Pair<String, Boolean?> = key to this?.asBoolean()
infix fun JsonNode?.asTimestampWithName(key: String): Pair<String, String?> = key to this?.asTimestamp()
