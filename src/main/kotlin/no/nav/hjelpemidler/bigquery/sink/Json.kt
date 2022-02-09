package no.nav.hjelpemidler.bigquery.sink

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonMapperBuilder
import com.fasterxml.jackson.module.kotlin.treeToValue
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

val jsonMapper: JsonMapper = jacksonMapperBuilder()
    .addModule(JavaTimeModule())
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .build()

fun JsonNode.asLocalDateTime(): LocalDateTime = LocalDateTime.parse(asText()).truncatedTo(ChronoUnit.MICROS)

fun JsonNode.asSchemaId(): SchemaId = asText().split("_v").let {
    SchemaId(it.first(), it.last().toInt())
}

fun JsonNode.asKeyValueMap(): Map<String, String> = jsonMapper.treeToValue(this)
