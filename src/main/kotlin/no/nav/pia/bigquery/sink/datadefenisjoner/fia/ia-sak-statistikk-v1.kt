package no.nav.pia.bigquery.sink.datadefenisjoner.fia

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import no.nav.pia.bigquery.sink.asDateTime
import no.nav.pia.bigquery.sink.asBigDecimal
import no.nav.pia.bigquery.sink.datadefenisjoner.toRowToInsert
import no.nav.pia.bigquery.sink.schema.SchemaDefinition
import no.nav.pia.bigquery.sink.schema.standardTableDefinition
import no.nav.pia.bigquery.sink.use

val `ia-sak-statistikk-v1` = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "ia-sak-statistikk",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            string("saksnummer") {
                required()
                description("Saksnummer")
            }
            string("orgnr") {
                required()
                description("Virksomhetsnummer (b.nummer)")
            }
            string("eierAvSak") {
                description("Navidenten til rådgiver som eier saken")
            }
            string("status") {
                required()
                description("Status som viser hvor i IA-prosessen man er")
            }
            string("endretAvHendelseId") {
                required()
                description("ID til hendelsen som som endret saken")
            }
            string("hendelse") {
                description("Hendelsen som som endret saken")
            }
            string("ikkeAktuelBegrunnelse") {
                description("Begrunnelse for at man ikke går videre med denne virksomheten")
            }
            timestamp("opprettetTidspunkt") {
                required()
                description("Tidspunkt for opprettelse av sak")
            }
            timestamp("endretTidspunkt") {
                required()
                description("Tidspunkt for siste oppdatering av sak")
            }
            timestamp("avsluttetTidspunkt") {
                description("Tidspunkt for avslutting av sak")
            }

            decimal("antallPersoner") {
                description("Antall arbeidsforhold tilknyttet virksomheten for siste kvartal")
            }
            decimal("tapteDagsverk") {
                description("Antall tapte dagsverk i virksomheten for siste kvartal")
            }
            decimal("muligeDagsverk") {
                description("Antall mulige dagsverk i virksomheten for siste kvartal")
            }
            decimal("sykefraversprosent") {
                description("Sykefraværsprosenten i virksomheten for siste kvartal")
            }
            integer("arstall") {
                description("Siste kvartals årstall - hører sammen med kvartal")
            }
            integer("kvartal") {
                description("Siste kvartal - hører sammen med arstall")
            }
            decimal("tapteDagsverkSiste4Kvartal") {
                description("Antall tapte dagsverk i virksomheten for siste 4 kvartal")
            }
            decimal("muligeDagsverkSiste4Kvartal") {
                description("Antall mulige dagsverk i virksomheten for siste 4 kvartal")
            }
            decimal("sykefraversprosentSiste4Kvartal") {
                description("Sykefraværsprosenten i virksomheten for siste 4 kvartal")
            }
            json("kvartaler") {
                required()
                description("Kvartaler som ingår i de siste 4 kvartaler")
            }
            string("sektor") {
                description("Sektor som virksomheten tilhører")
            }
            json("neringer") {
                required()
                description("Næringer som virksomheten tilhører")
            }
            string("bransjeprogram") {
                description("Bransjeprogram som virksomheten tilhører. Definert i IA-avtalen")
            }
            string("postnummer") {
                description("Postnummer til virksomheten")
            }
            string("kommunenummer") {
                description("Kommunenummer til virksomheten")
            }
            string("fylkesnummer") {
                description("Fylkesnummer til virksomheten")
            }

            timestamp("tidsstempel") {
                required()
                description("Tidsstempel for lagring i BigQuery")
            }
        }
    }

    override fun transform(payload: JsonNode): InsertAllRequest.RowToInsert = mapOf(
        payload.use("saksnummer") { textValue() },
        payload.use("orgnr") { textValue() },
        payload.use("eierAvSak") { textValue() },
        payload.use("status") { textValue() },
        payload.use("endretAvHendelseId") { textValue() },
        payload.use("hendelse") { textValue() },
        payload.use("ikkeAktuelBegrunnelse") { textValue() },
        payload.use("opprettetTidspunkt") { asDateTime() },
        payload.use("endretTidspunkt") { asDateTime() },
        payload.use("avsluttetTidspunkt") { asDateTime() },
        payload.use("antallPersoner") { asBigDecimal() },
        payload.use("tapteDagsverk") { asBigDecimal() },
        payload.use("muligeDagsverk") { asBigDecimal() },
        payload.use("sykefraversprosent") { asBigDecimal() },
        payload.use("arstall") { intValue() },
        payload.use("kvartal") { intValue() },
        payload.use("tapteDagsverkSiste4Kvartal") { asBigDecimal() },
        payload.use("muligeDagsverkSiste4Kvartal") { asBigDecimal() },
        payload.use("sykefraversprosentSiste4Kvartal") { asBigDecimal() },
        payload.use("kvartaler") { textValue() },
        payload.use("sektor") { textValue() },
        payload.use("neringer") { textValue() },
        payload.use("bransjeprogram") { textValue() },
        payload.use("postnummer") { textValue() },
        payload.use("kommunenummer") { textValue() },
        payload.use("fylkesnummer") { textValue() },
        "tidsstempel" to "AUTO",
    ).toRowToInsert()
}
