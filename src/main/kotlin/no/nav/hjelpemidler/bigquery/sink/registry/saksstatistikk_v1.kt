package no.nav.hjelpemidler.bigquery.sink.registry

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TimePartitioning
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.toBoolean
import no.nav.hjelpemidler.bigquery.sink.toText
import no.nav.hjelpemidler.bigquery.sink.toTimestamp

val saksstatistikk_v1 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "saksstatistikk",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            string("sak_id") {
                required()
                description("Fagsystemets saks-ID")
            }
            string("behandling_id") {
                required()
                description("Fagsystemets behandlings-ID")
            }
            string("saksbehandler") {
                nullable()
                description("Azure-objekt-ID for saksbehandler som sist var involvert i behandlingen")
            }
            string("saksbehandler_ident") {
                nullable()
                description("Saksbehandler-ID for saksbehandler som sist var involvert i behandlingen")
            }
            string("person_ident") {
                required()
                description("PersonIdent tilknyttet søker eller hovedaktør for ytelsen")
            }
            timestamp("registrert_tid") {
                required()
                description("Tidspunktet da behandlingen først oppstod eller ble registrert i fagsystemet")
            }
            timestamp("endret_tid") {
                required()
                description("Tidspunktet for siste endring på behandlingen. Ved første melding vil denne være lik registrert_tid.")
            }
            timestamp("teknisk_tid") {
                required()
                description("Tidspunktet da fagsystemet legger hendelsen på grensesnittet")
            }
            timestamp("mottatt_tid") {
                nullable()
                description("Tidspunktet da behandlingen oppstår (eks. søknadstidspunkt, inntektsmelding, etc.). Det er ønskelig å måle brukers opplevde ventetid. Ved elektronisk kontakt regner vi med at denne er lik registrert_tid.")
            }
            timestamp("ferdig_behandlet_tid") {
                nullable()
                description("Tidspunktet da behandling ble avsluttet, enten avbrutt, henlagt, vedtak innvilget/avslått, etc.")
            }
            timestamp("vedtak_tid") {
                nullable()
                description("Tidspunktet for når vedtaket ble fattet - hvis saken ble vedtatt")
            }
            string("sak_ytelse") {
                required()
                description("Kode som angir hvilken ytelse/stønad behandlingen gjelder")
            }
            string("under_type") {
                required()
                description("Kode som angir hvilke type hjelpemidler behandlingen gjelder")
            }
            string("behandling_type") {
                required()
                description("Kode som angir hvilken type behandling det er snakk om - typisk: søknad, revurdering, tilbakekreving, klage, etc.")
            }
            string("behandling_metode") {
                required()
                description("Kode som angir automatiseringsgrad, f.eks. MANUELL og AUTO")
            }
            string("behandling_status") {
                required()
                description("Kode som angir hvilken status behandlingen har - typisk: opprettet, under behandling, avsluttet, etc.")
            }
            string("behandling_resultat") {
                nullable()
                description("Kode som angir resultatet på behandling - typisk: avbrutt, innvilget, delvis innvilget, henlagt av tekniske hensyn, etc.")
            }
            string("opprettet_av") {
                required()
                description("Saksbehandler-ID som opprettet behandlingen. Hvis det er en service-bruker så send denne.")
            }
            string("opprettet_enhet") {
                required()
                description("Hvilken org. enhet som behandlingen opprinnelig ble rutet til i NAV. Dette kan også være en nasjonal kø.")
            }
            string("ansvarlig_enhet") {
                required()
                description("Hvilken org. enhet som nå har ansvar for saken. Dette kan være samme som opprettet_enhet. Avslåtte klager i vedtaksinstans skal ha riktig KA-enhet her.")
            }
            boolean("totrinnsbehandling") {
                required()
                description("Hvis det er utført totrinnskontroll skal denne være true.")
            }
            string("sak_utland") {
                required()
                description("Kode som angir om behandlingen er knyttet til person har hatt bosted, opphold eller arbeid i utlandet, og der dette forholdet kan påvirke behandlingen. Se begrepskatalogen: https://jira.adeo.no/browse/BEGREP-1611#")
            }
            string("avsender") {
                required()
                description("Angir fagsystemets eget navn")
            }
            string("versjon") {
                required()
                description("Kode som angir hvilken versjon av koden dataene er generert på bakgrunn av")
            }
            timestamp("tidsstempel") {
                required()
                description("Tidsstempel for lagring av raden")
            }
        }
        timePartitioning(TimePartitioning.Type.MONTH) {
            setField("endret_tid")
        }
        clustering {
            setFields(listOf("endret_tid"))
        }
    }

    override fun transform(payload: JsonNode): RowToInsert = mapOf(
        payload["sakId"] toText "sak_id",
        payload["behandlingId"] toText "behandling_id",
        payload["saksbehandler"] toText "saksbehandler",
        payload["saksbehandlerIdent"] toText "saksbehandler_ident",
        payload["personIdent"] toText "person_ident",
        payload["registrertTid"] toTimestamp "registrert_tid",
        payload["endretTid"] toTimestamp "endret_tid",
        payload["tekniskTid"] toTimestamp "teknisk_tid",
        payload["mottattTid"] toTimestamp "mottatt_tid",
        payload["ferdigBehandletTid"] toTimestamp "ferdig_behandlet_tid",
        payload["vedtakTid"] toTimestamp "vedtak_tid",
        payload["sakYtelse"] toText "sak_ytelse",
        payload["underType"] toText "under_type",
        payload["behandlingType"] toText "behandling_type",
        payload["behandlingMetode"] toText "behandling_metode",
        payload["behandlingStatus"] toText "behandling_status",
        payload["behandlingResultat"] toText "behandling_resultat",
        payload["opprettetAv"] toText "opprettet_av",
        payload["opprettetEnhet"] toText "opprettet_enhet",
        payload["ansvarligEnhet"] toText "ansvarlig_enhet",
        payload["totrinnsbehandling"] toBoolean "totrinnsbehandling",
        payload["sakUtland"] toText "sak_utland",
        payload["avsender"] toText "avsender",
        payload["versjon"] toText "versjon",
        "tidsstempel" to "AUTO",
    ).toRowToInsert()
}
