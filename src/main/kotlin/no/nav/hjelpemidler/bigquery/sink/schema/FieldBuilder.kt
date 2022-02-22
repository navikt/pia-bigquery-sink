package no.nav.hjelpemidler.bigquery.sink.schema

import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.StandardSQLTypeName

class FieldBuilder(private val name: String, private val type: StandardSQLTypeName) {
    private var mode: Field.Mode = Field.Mode.NULLABLE
    private var description: String? = null
    private var subFields: FieldList? = null

    fun nullable() {
        this.mode = Field.Mode.NULLABLE
    }

    fun required() {
        this.mode = Field.Mode.REQUIRED
    }

    fun repeated() {
        this.mode = Field.Mode.REPEATED
    }

    fun description(description: String) {
        this.description = description
    }

    fun subFields(block: SchemaBuilder.() -> Unit) {
        this.subFields = SchemaBuilder().apply(block).fieldList()
    }

    fun build(): Field = Field.newBuilder(name, type, subFields)
        .setMode(mode)
        .setDescription(description)
        .build()
}
