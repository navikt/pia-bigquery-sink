package no.nav.pia.bigquery.sink.schema

import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardSQLTypeName

class SchemaBuilder {
    private val fields = mutableListOf<Field>()

    fun fieldList(): FieldList = FieldList.of(fields)

    fun build(): Schema = Schema.of(fields)

    private fun field(
        name: String,
        type: StandardSQLTypeName,
        block: FieldBuilder.() -> Unit = {},
    ): Field =
        FieldBuilder(name, type)
            .apply(block)
            .build()
            .also { fields.add(it) }

    fun boolean(
        name: String,
        block: FieldBuilder.() -> Unit = {},
    ): Field = field(name, StandardSQLTypeName.BOOL, block)

    fun integer(
        name: String,
        block: FieldBuilder.() -> Unit = {},
    ): Field = field(name, StandardSQLTypeName.INT64, block)

    fun string(
        name: String,
        block: FieldBuilder.() -> Unit = {},
    ): Field = field(name, StandardSQLTypeName.STRING, block)

    fun json(
        name: String,
        block: FieldBuilder.() -> Unit = {},
    ): Field = field(name, StandardSQLTypeName.JSON, block)

    fun timestamp(
        name: String,
        block: FieldBuilder.() -> Unit = {},
    ): Field = field(name, StandardSQLTypeName.TIMESTAMP, block)

    fun date(
        name: String,
        block: FieldBuilder.() -> Unit = {},
    ): Field = field(name, StandardSQLTypeName.DATE, block)

    fun decimal(
        name: String,
        block: FieldBuilder.() -> Unit = {},
    ): Field = field(name, StandardSQLTypeName.NUMERIC, block)
}
