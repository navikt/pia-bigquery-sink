package no.nav.pia.bigquery.sink.datadefenisjoner.fia

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import no.nav.pia.bigquery.sink.datadefenisjoner.toRowToInsert
import no.nav.pia.bigquery.sink.schema.SchemaDefinition
import no.nav.pia.bigquery.sink.schema.standardTableDefinition
import no.nav.pia.bigquery.sink.use

val ia_sak_hendelser_v1 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "ia_sak_hendelse",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            string("orgnr") {
                required()
                description("Orgnr. for virksomheten")
            }
            string("navn") {
                required()
                description("Navn på virksomheten")
            }
            timestamp("tidsstempel") {
                required()
                description("Tidsstempel for lagring av avtalen")
            }
            datetime("status") {
                required()
                description("Status som er resultatet av oppdateringen")
            }
        }
    }

    override fun transform(payload: JsonNode): InsertAllRequest.RowToInsert = mapOf(
        payload.use("orgnr") { textValue() },
        payload.use("navn") { textValue() },
        payload.use("status") { textValue() },
        "tidsstempel" to "AUTO",
    ).toRowToInsert()
}
