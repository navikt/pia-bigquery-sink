package no.nav.hjelpemidler.bigquery.sink.schema

import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.StandardSQLTypeName

@Target(AnnotationTarget.CLASS)
@MustBeDocumented
annotation class SchemaTable(
    val name: String,
    val version: Int,
    val description: String = "",
)

@Target(AnnotationTarget.PROPERTY)
@MustBeDocumented
annotation class SchemaField(
    val name: String,
    val description: String = "",
    val type: StandardSQLTypeName,
    val mode: Mode = Mode.NULLABLE,
)
