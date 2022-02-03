package no.nav.hjelpemidler.bigquery.sink

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonMapperBuilder
import com.fasterxml.jackson.module.kotlin.treeToValue
import com.google.cloud.bigquery.InsertAllRequest

val jsonMapper: JsonMapper = jacksonMapperBuilder()
    .addModule(JavaTimeModule())
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .build()

inline fun <reified T> JsonNode.asObject() = jsonMapper.treeToValue<T>(this)

fun JsonNode.asMap() = when {
    isArray -> null
    isObject -> jsonMapper.treeToValue<Map<String, Any?>>(this)
    else -> InsertAllRequest.RowToInsert.of(emptyMap<String, String>())
}
