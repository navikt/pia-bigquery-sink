package no.nav.hjelpemidler.bigquery.sink

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonMapperBuilder
import com.fasterxml.jackson.module.kotlin.treeToValue
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaId
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

val jsonMapper: JsonMapper = jacksonMapperBuilder()
    .addModule(JavaTimeModule())
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .build()

inline fun <reified T> JsonNode.asObject() = jsonMapper.treeToValue<T>(this)

fun JsonNode.asLocalDateTime(): LocalDateTime = LocalDateTime.parse(asText())

fun JsonNode.asDateTime(): String = asLocalDateTime().truncatedTo(ChronoUnit.MICROS).toString()

fun JsonNode.asSchemaId(): SchemaId = SchemaId.of(asText())

fun <T> JsonNode.use(key: String, transform: JsonNode.() -> T): Pair<String, T?> = key to get(key)?.let {
    transform(it)
}
