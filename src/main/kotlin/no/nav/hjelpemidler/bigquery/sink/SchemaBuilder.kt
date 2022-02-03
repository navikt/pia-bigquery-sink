package no.nav.hjelpemidler.bigquery.sink

import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardSQLTypeName
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo

class SchemaBuilder {
    private val fields = mutableListOf<Field>()

    fun build(): Schema = Schema.of(fields)

    private fun field(
        name: String,
        type: StandardSQLTypeName,
        block: Field.Builder.() -> Unit = {},
    ): Field = Field.newBuilder(name, type)
        .apply(block)
        .build()
        .also { fields.add(it) }

    fun datetime(
        name: String,
        block: Field.Builder.() -> Unit = {},
    ): Field = field(name, StandardSQLTypeName.DATETIME, block)

    fun string(
        name: String,
        block: Field.Builder.() -> Unit = {},
    ): Field = field(name, StandardSQLTypeName.STRING, block)

    fun timestamp(
        name: String,
        block: Field.Builder.() -> Unit = {},
    ): Field = field(name, StandardSQLTypeName.TIMESTAMP, block)

    fun Field.Builder.nullable(): Field.Builder = setMode(Mode.NULLABLE)
    fun Field.Builder.required(): Field.Builder = setMode(Mode.REQUIRED)
    fun Field.Builder.repeated(): Field.Builder = setMode(Mode.REPEATED)
}

fun table(tableId: TableId, block: SchemaBuilder.() -> Unit): TableInfo {
    val schemaBuilder = SchemaBuilder().apply(block)
    val tableDefinition = StandardTableDefinition.of(schemaBuilder.build())
    return TableInfo.newBuilder(tableId, tableDefinition).build()
}
