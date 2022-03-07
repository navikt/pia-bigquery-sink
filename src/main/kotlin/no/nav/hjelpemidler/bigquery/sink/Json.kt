package no.nav.hjelpemidler.bigquery.sink

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonMapperBuilder
import com.fasterxml.jackson.module.kotlin.treeToValue
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

val jsonMapper: JsonMapper = jacksonMapperBuilder()
    .addModule(JavaTimeModule())
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .build()

inline fun <reified T> JsonNode.asObject(): T = jsonMapper.treeToValue(this)

fun JsonNode.asHash(): String = Hash.encode(asText())
fun JsonNode.asLocalDateTime(): LocalDateTime = LocalDateTime.parse(asText())
fun JsonNode.asZonedDateTime(): ZonedDateTime = ZonedDateTime.parse(asText())
fun JsonNode.asDateTime(): String = asLocalDateTime().truncatedTo(ChronoUnit.MICROS).toString()
fun JsonNode.asTimestamp(): String = asZonedDateTime().truncatedTo(ChronoUnit.MICROS).toInstant().toString()
fun JsonNode.asSchemaId(): SchemaDefinition.Id = SchemaDefinition.Id.of(asText())
fun JsonNode.asMap(): Map<String, Any?> = asObject()

fun <T> JsonNode.use(key: String, transform: JsonNode.() -> T): Pair<String, T?> = key to get(key)?.let {
    transform(it)
}

infix fun JsonNode?.toText(key: String): Pair<String, String?> = key to this?.asText()
infix fun JsonNode?.toHash(key: String): Pair<String, String?> = key to this?.asHash()
infix fun JsonNode?.toBoolean(key: String): Pair<String, Boolean?> = key to this?.asBoolean()
infix fun JsonNode?.toDateTime(key: String): Pair<String, String?> = key to this?.asDateTime()
infix fun JsonNode?.toTimestamp(key: String): Pair<String, String?> = key to this?.asTimestamp()
