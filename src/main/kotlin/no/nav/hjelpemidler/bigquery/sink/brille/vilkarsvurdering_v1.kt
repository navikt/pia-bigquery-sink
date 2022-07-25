package no.nav.hjelpemidler.bigquery.sink.brille

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.registry.toRowToInsert
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.use

val vilkarsvurdering_v1 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "vilkarsvurdering",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            decimal("hoyre_sfere") {
                required()
            }
            decimal("hoyre_sylinder") {
                required()
            }
            decimal("venstre_sfere") {
                required()
            }
            decimal("venstre_sylinder") {
                required()
            }
            datetime("opprettet") {
                required()
                description("Tidsstempel for opprettelse av vilkårsvurderingen")
            }
            timestamp("tidsstempel") {
                required()
                description("Tidsstempel for lagring av vilkårsvurderingen")
            }
        }
    }

    override fun transform(payload: JsonNode): InsertAllRequest.RowToInsert = mapOf(
        payload.use("opprettet") { asDateTime() },
        "tidsstempel" to "AUTO",
    ).toRowToInsert()
}
