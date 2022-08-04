package no.nav.hjelpemidler.bigquery.sink.brille

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.contains
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TimePartitioning
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.registry.toRowToInsert
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.use

val avslag_v1 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "avslag",
        version = 1
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            boolean("har_ikke_vedtak_i_kalenderaret_oppfylt") {
                required()
                description("Om barnet ikke allerede har et vedtak i samme kalenderår som bestillingsdato")
            }
            boolean("under_18_ar_pa_bestillingsdato_oppfylt") {
                required()
                description("Om barnet er under 18 år på bestillingsdato")
            }
            boolean("medlem_av_folketrygden_oppfylt") {
                required()
                description("Om barnet er medlem av folketrygden")
            }
            boolean("brillestyrke_oppfylt") {
                required()
                description("Om oppgitt brillestyrke er innenfor det ordningen dekker")
            }
            boolean("bestillingsdato_oppfylt") {
                required()
                description("Om bestillingsdato er etter ordningen startet, men ikke i fremtiden")
            }
            boolean("bestillingsdato_tilbake_i_tid_oppfylt") {
                required()
                description("Om bestillingsdato er max. 6 mnd tilbake i tid")
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
        timePartitioning(TimePartitioning.Type.MONTH) {
            setField("opprettet")
        }
        clustering {
            setFields(listOf("opprettet"))
        }
    }

    override fun transform(payload: JsonNode): InsertAllRequest.RowToInsert = mapOf(
        payload.use("opprettet") { asDateTime() },
        payload.use("har_ikke_vedtak_i_kalenderaret_oppfylt") { booleanValue() },
        payload.use("under_18_ar_pa_bestillingsdato_oppfylt") { booleanValue() },
        payload.use("medlem_av_folketrygden_oppfylt") { booleanValue() },
        payload.use("brillestyrke_oppfylt") { booleanValue() },
        payload.use("bestillingsdato_oppfylt") { booleanValue() },
        payload.use("bestillingsdato_tilbake_i_tid_oppfylt") { booleanValue() },
        "tidsstempel" to "AUTO"
    ).toRowToInsert()

    override fun skip(payload: JsonNode): Boolean = payload.contains("brilleseddel") // gammel event fra hm-brille-api
}
