package no.nav.hjelpemidler.bigquery.sink.brille

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TimePartitioning
import no.nav.hjelpemidler.bigquery.sink.asDate
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.registry.toRowToInsert
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.use

val vedtak_v1 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "vedtak",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            string("orgnr") {
                required()
            }
            string("org_navn") {
                required()
            }
            integer("barnets_alder") {
                nullable()
            }
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
            date("bestillingsdato") {
                required()
            }
            decimal("brillepris") {
                required()
            }
            string("behandlingsresultat") {
                required()
            }
            string("sats") {
                required()
            }
            integer("sats_belop") {
                required()
            }
            string("sats_beskrivelse") {
                required()
            }
            decimal("belop") {
                required()
            }
            datetime("opprettet") {
                required()
            }
            timestamp("tidsstempel") {
                required()
            }
        }
        timePartitioning(TimePartitioning.Type.MONTH) {
            setField("opprettet")
        }
        clustering {
            setFields(listOf("opprettet"))
        }
    }

    override fun transform(payload: JsonNode): InsertAllRequest.RowToInsert = mapOf(
        payload.use("opprettet") { asDateTime() },
        payload.use("orgnr") { textValue() },
        payload.use("org_navn") { textValue() },
        payload.use("barnets_alder") { intValue() },
        payload.use("hoyre_sfere") { decimalValue() },
        payload.use("hoyre_sylinder") { decimalValue() },
        payload.use("venstre_sfere") { decimalValue() },
        payload.use("venstre_sylinder") { decimalValue() },
        payload.use("bestillingsdato") { asDate() },
        payload.use("brillepris") { decimalValue() },
        payload.use("behandlingsresultat") { textValue() },
        payload.use("sats") { textValue() },
        payload.use("sats_belop") { intValue() },
        payload.use("sats_beskrivelse") { textValue() },
        payload.use("belop") { decimalValue() },
        "tidsstempel" to "AUTO",
    ).toRowToInsert()
}
