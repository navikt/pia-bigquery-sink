package no.nav.hjelpemidler.bigquery.sink.brille

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.contains
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TimePartitioning
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.asLocalDate
import no.nav.hjelpemidler.bigquery.sink.registry.toRowToInsert
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.use
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

val avslag_v1 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "avslag",
        version = 1
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            string("orgnr") {
                required()
                description("Orgnr. for virksomheten som gjorde oppslaget")
            }
            string("navn") {
                required()
                description("Navn på virksomheten som gjorde oppslaget")
            }
            boolean("har_ikke_vedtak_ikalenderaret_oppfylt") {
                required()
                description("Om barnet ikke allerede har et vedtak i samme kalenderår som bestillingsdato")
            }
            boolean("under18_ar_pa_bestillingsdato_oppfylt") {
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
            boolean("bestillingsdato_tilbake_itid_oppfylt") {
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
    }

    override fun transform(payload: JsonNode): InsertAllRequest.RowToInsert = mapOf(
        payload.use("orgnr") { textValue() },
        payload.use("navn") { textValue() },
        payload.use("har_ikke_vedtak_ikalenderaret_oppfylt") { booleanValue() },
        payload.use("under18_ar_pa_bestillingsdato_oppfylt") { booleanValue() },
        payload.use("medlem_av_folketrygden_oppfylt") { booleanValue() },
        payload.use("brillestyrke_oppfylt") { booleanValue() },
        payload.use("bestillingsdato_oppfylt") { booleanValue() },
        payload.use("bestillingsdato_tilbake_itid_oppfylt") { booleanValue() },
        payload.use("opprettet") { LocalDateTime.now().truncatedTo(ChronoUnit.MICROS).toString() }, // midlertidig for å ta unna feilende meldinger
        "tidsstempel" to "AUTO"
    ).toRowToInsert()

    override fun skip(payload: JsonNode): Boolean = payload.contains("brilleseddel") // gammel event fra hm-brille-api
}
