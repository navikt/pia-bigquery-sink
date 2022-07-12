package no.nav.hjelpemidler.bigquery.sink.brille

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.registry.toRowToInsert
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.use

val avtale_v1 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "avtale",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            string("orgnr") {
                required()
                description("Orgnr. for virksomheten det er inngått avtale med")
            }
            string("navn") {
                required()
                description("Navn på virksomheten det er inngått avtale med")
            }
            datetime("opprettet") {
                required()
                description("Tidsstempel for opprettelse av avtalen")
            }
            timestamp("tidsstempel") {
                required()
                description("Tidsstempel for lagring av avtalen")
            }
        }
    }

    override fun transform(payload: JsonNode): InsertAllRequest.RowToInsert = mapOf(
        payload.use("orgnr") { textValue() },
        payload.use("navn") { textValue() },
        payload.use("opprettet") { asDateTime() },
        "tidsstempel" to "AUTO",
    ).toRowToInsert()
}
