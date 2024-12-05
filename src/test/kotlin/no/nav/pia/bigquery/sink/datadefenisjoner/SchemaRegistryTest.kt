package no.nav.pia.bigquery.sink.datadefenisjoner

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.bigquery.DatasetId
import io.kotest.matchers.maps.shouldContain
import io.kotest.matchers.shouldBe
import kotlinx.serialization.json.Json
import no.nav.pia.bigquery.sink.SamarbeidsplanConsumer.PlanKafkamelding
import no.nav.pia.bigquery.sink.SpørreundersøkelseConsumer.SpørreundersøkelseEksport
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`ia-sak-leveranse-v1`
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`ia-sak-statistikk-v1`
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`samarbeid-bigquery-v1`
import no.nav.pia.bigquery.sink.oversettFraCetTilUtc
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

internal class SchemaRegistryTest {
    @Test
    internal fun `creating arbitrary TableInfo should not fail`() {
        val definition = schemaRegistry.values.first()
        val datasetId = DatasetId.of("project", "dataset")
        val tableInfo = definition.toTableInfo(datasetId)
        tableInfo.tableId shouldBe definition.schemaId.toTableId(datasetId)
    }

    @Test
    internal fun `payload transformed to ia_sak_hendelser_v1 row`() {
        val now = ZonedDateTime.now(ZoneId.of("Europe/Oslo")).toLocalDateTime()
        val payload = ObjectMapper().readTree(
            """{
             "saksnummer": "123456789",
             "orgnr": "123456789",
             "eierAvSak": "N123456",
             "endretAvHendelseId": "123456789",
             "status": "VI_BISTÅR",
             "opprettetTidspunkt": "$now",
             "endretTidspunkt": "$now",
             "avsluttetTidspunkt": null,
             "sykefraversprosentSiste4Kvartal": 10.4836642887593
             
            }
            """.trimMargin(),
        )

        val content = `ia-sak-statistikk-v1`.transform(payload).content
        content.shouldContain("saksnummer" to "123456789")
        content.shouldContain("orgnr" to "123456789")
        content.shouldContain("eierAvSak" to "N123456")
        content.shouldContain("endretAvHendelseId" to "123456789")
        content.shouldContain("status" to "VI_BISTÅR")
        content.shouldContain("opprettetTidspunkt" to "${now.oversettFraCetTilUtc().truncatedTo(ChronoUnit.MICROS)}")
        content.shouldContain("endretTidspunkt" to "${now.oversettFraCetTilUtc().truncatedTo(ChronoUnit.MICROS)}")
        content.shouldContain("sykefraversprosent" to null)
        content.shouldContain("tidsstempel" to "AUTO")
    }

    @Test
    internal fun `real payload transformed to ia_sak_hendelser_v1 row`() {
        val payload = ObjectMapper().readTree(
            """
            {
            "saksnummer":"01GVDFDVA8DKN811GBQT0W065J",
            "orgnr":"91234569",
            "eierAvSak":null,
            "status":"NY",
            "endretAvHendelseId":"01GVDFDVA8DKN811GBQT0W065J",
            "hendelse":"OPPRETT_SAK_FOR_VIRKSOMHET",
            "endretAv":"Z12345",
            "endretAvRolle":"SUPERBRUKER",
            "ikkeAktuelBegrunnelse":null,
            "opprettetTidspunkt":"2023-03-13T13:34:21.128356",
            "endretTidspunkt":"2023-03-13T13:34:21.128356",
            "avsluttetTidspunkt":null,
            "antallPersoner":234.0,
            "tapteDagsverk":216.735876472921,
            "tapteDagsverkGradert":108.36793823646,
            "muligeDagsverk":6314.79015997463,
            "sykefraversprosent":14.6662125361897,
            "graderingsprosent":50.0,
            "arstall":2022,
            "kvartal":4,
            "tapteDagsverkSiste4Kvartal":1000.0,
            "tapteDagsverkGradertSiste4Kvartal":600.0,
            "muligeDagsverkSiste4Kvartal":9434.0,
            "sykefraversprosentSiste4Kvartal":10.4836642887593,
            "graderingsprosentSiste4Kvartal":60.0,
            "kvartaler":[{"årstall":2021,"kvartal":2},{"årstall":2021,"kvartal":3},{"årstall":2021,"kvartal":4},{"årstall":2022,"kvartal":1}],
            "sektor":"PRIVAT",
            "neringer":[{"navn":"Utleie av egen eller leid fast eiendom ellers","kode":"68.209"}],
            "bransjeprogram":null,
            "postnummer":"6240",
            "kommunenummer":"1507",
            "fylkesnummer":"15"
            }
            """.trimIndent(),
        )
        val content = `ia-sak-statistikk-v1`.transform(payload).content
        content.shouldContain("saksnummer" to "01GVDFDVA8DKN811GBQT0W065J")
        content.shouldContain("orgnr" to "91234569")
        content.shouldContain("eierAvSak" to null)
        content.shouldContain("endretAvHendelseId" to "01GVDFDVA8DKN811GBQT0W065J")
        content.shouldContain("endretAv" to "Z12345")
        content.shouldContain("endretAvRolle" to "SUPERBRUKER")
        content.shouldContain("status" to "NY")
        content.shouldContain("opprettetTidspunkt" to "2023-03-13T12:34:21.128356")
        content.shouldContain("endretTidspunkt" to "2023-03-13T12:34:21.128356")
        content.shouldContain("tapteDagsverk" to BigDecimal("216.7"))
        content.shouldContain("tapteDagsverkGradert" to BigDecimal("108.4"))
        content.shouldContain("sykefraversprosent" to BigDecimal("14.7"))
        content.shouldContain("graderingsprosent" to BigDecimal("50.0"))
        content.shouldContain("tapteDagsverkSiste4Kvartal" to BigDecimal("1000.0"))
        content.shouldContain("tapteDagsverkGradertSiste4Kvartal" to BigDecimal("600.0"))
        content.shouldContain("sykefraversprosentSiste4Kvartal" to BigDecimal("10.5"))
        content.shouldContain("graderingsprosentSiste4Kvartal" to BigDecimal("60.0"))
        content.shouldContain("tidsstempel" to "AUTO")
    }

    @Test
    internal fun `real payload with null values transformed to ia_sak_hendelser_v1 row`() {
        val payload = ObjectMapper().readTree(
            """
            {
            "saksnummer":"01G29XHZ7JDC1511RXC6WN1WRV",
            "orgnr":"91234568",
            "eierAvSak":null,
            "status":"NY",
            "endretAvHendelseId":"01G29XHZ7JDC1511RXC6WN1WRV",
            "hendelse":"OPPRETT_SAK_FOR_VIRKSOMHET",
            "endretAv":"Z994537",
            "endretAvRolle":null,
            "ikkeAktuelBegrunnelse":null,
            "opprettetTidspunkt":"2022-05-05T12:51:52.946658",
            "endretTidspunkt":"2022-05-05T12:51:52.946658",
            "avsluttetTidspunkt":null,
            "antallPersoner":null,
            "tapteDagsverk":null,
            "tapteDagsverkGradert":null,
            "muligeDagsverk":null,
            "sykefraversprosent":null,
            "graderingsprosent":null,
            "arstall":null,
            "kvartal":null,
            "tapteDagsverkSiste4Kvartal":null,
            "tapteDagsverkGradertSiste4Kvartal":null,
            "muligeDagsverkSiste4Kvartal":null,
            "sykefraversprosentSiste4Kvartal":null,
            "graderingsprosentSiste4Kvartal":null,
            "kvartaler":[],
            "sektor":"STATLIG",
            "neringer":[{"navn":"Alminnelige somatiske sykehus","kode":"86.101"}],
            "bransjeprogram":"SYKEHUS",
            "postnummer":"5021",
            "kommunenummer":"4601",
            "fylkesnummer":"46"}
            """.trimIndent(),
        )
        val content = `ia-sak-statistikk-v1`.transform(payload).content
        content.shouldContain("saksnummer" to "01G29XHZ7JDC1511RXC6WN1WRV")
        content.shouldContain("orgnr" to "91234568")
        content.shouldContain("eierAvSak" to null)
        content.shouldContain("endretAvHendelseId" to "01G29XHZ7JDC1511RXC6WN1WRV")
        content.shouldContain("endretAv" to "Z994537")
        content.shouldContain("endretAvRolle" to null)
        content.shouldContain("status" to "NY")
        content.shouldContain("tidsstempel" to "AUTO")
    }

    @Test
    internal fun `kan transformere ia-sak-leveranse melding`() {
        val json = ObjectMapper().readTree(
            """
            {
                "id":1,
                "saksnummer":"01GVJFE0REVM09011RS6B11X46",
                "iaTjenesteId":3,
                "iaTjenesteNavn":"Redusere sykefravær",
                "iaModulId":2,
                "iaModulNavn":"Videreutvikle sykefraværsrutiner",
                "frist":"2023-03-15",
                "status":"UNDER_ARBEID",
                "opprettetAv":"X12345",
                "sistEndret":"2023-03-15T12:10:39.369468",
                "sistEndretAv":"X12345",
                "sistEndretAvRolle":"SAKSBEHANDLER",
                "fullført":null,
                "opprettetTidspunkt":"2023-03-15T12:10:39.369468"
            }
            """.trimIndent(),
        )

        val content = `ia-sak-leveranse-v1`.transform(json).content
        content.shouldContain("id" to 1)
        content.shouldContain("saksnummer" to "01GVJFE0REVM09011RS6B11X46")
        content.shouldContain("iaTjenesteId" to 3)
        content.shouldContain("iaTjenesteNavn" to "Redusere sykefravær")
        content.shouldContain("iaModulId" to 2)
        content.shouldContain("iaModulNavn" to "Videreutvikle sykefraværsrutiner")
        content.shouldContain("frist" to "2023-03-15")
        content.shouldContain("status" to "UNDER_ARBEID")
        content.shouldContain("opprettetAv" to "X12345")
        content.shouldContain("sistEndret" to "2023-03-15T11:10:39.369468")
        content.shouldContain("sistEndretAv" to "X12345")
        content.shouldContain("sistEndretAvRolle" to "SAKSBEHANDLER")
        content.shouldContain("fullfort" to null)
        content.shouldContain("opprettetTidspunkt" to "2023-03-15T11:10:39.369468")
    }

    @Test
    internal fun `kan transformere spørreundersøkelse melding`() {
        val melding =
            """
            {
                "id": "ada47eaf-8171-44ef-91b7-12f776d70545",
                "orgnr": "315901332",
                "type": "Behovsvurdering",
                "status": "AVSLUTTET",
                "samarbeidId": 427,
                "saksnummer": "01JDMA0Z3A50MN1V2C2GGD1YG9",
                "opprettetAv": "Z994459",
                "opprettet": "2024-11-26T14:19:59.247777",
                "harMinstEttSvar": false
            }
            """.trimIndent()

        val json = Json {
            ignoreUnknownKeys = true
        }

        val content = json.decodeFromString<SpørreundersøkelseEksport>(melding)

        content.id shouldBe "ada47eaf-8171-44ef-91b7-12f776d70545"
    }

    @Test
    internal fun `kan transformere samarbeid melding`() {
        val json = ObjectMapper().readTree(
            """
            {
                "id": 5,
                "saksnummer":"01GVJFE0REVM09011RS6B11X46",
                "navn": "Samarbeid Med Navn",
                "status": "SLETTET"
            }
            """.trimIndent(),
        )

        val content = `samarbeid-bigquery-v1`.transform(json).content
        content.shouldContain("id" to 5)
        content.shouldContain("saksnummer" to "01GVJFE0REVM09011RS6B11X46")
        content.shouldContain("navn" to "Samarbeid Med Navn")
        content.shouldContain("status" to "SLETTET")
    }

    @Test
    internal fun `kan transformere samarbeidsplan melding`() {
        val melding =
            """
            {
              "id": "09c9248f-76d1-40e9-9399-975a452b6e9a",
              "samarbeidId": 381,
              "sistEndret": "2024-11-14T16:34:09.494447",
              "temaer": [
                {
                  "id": 496,
                  "navn": "Partssamarbeid",
                  "inkludert": true,
                  "innhold": [
                    {
                      "id": 1805,
                      "navn": "Utvikle partssamarbeidet",
                      "inkludert": true
                    }
                  ]
                },
                {
                  "id": 497,
                  "navn": "Sykefraværsarbeid",
                  "inkludert": true,
                  "innhold": [
                    {
                      "id": 1806,
                      "navn": "Sykefraværsrutiner",
                      "inkludert": true
                    },
                    {
                      "id": 1808,
                      "navn": "Tilretteleggings- og medvirkningsplikt",
                      "inkludert": false
                    },
                    {
                      "id": 1809,
                      "navn": "Sykefravær - enkeltsaker",
                      "inkludert": true
                    },
                    {
                      "id": 1807,
                      "navn": "Oppfølgingssamtaler",
                      "inkludert": true
                    }
                  ]
                },
                {
                  "id": 498,
                  "navn": "Arbeidsmiljø",
                  "inkludert": true,
                  "innhold": [
                    {
                      "id": 1812,
                      "navn": "Oppfølging av arbeidsmiljøundersøkelser",
                      "inkludert": true
                    },
                    {
                      "id": 1811,
                      "navn": "Endring og omstilling",
                      "inkludert": false
                    },
                    {
                      "id": 1813,
                      "navn": "Livsfaseorientert personalpolitikk",
                      "inkludert": false
                    },
                    {
                      "id": 1814,
                      "navn": "Psykisk helse",
                      "inkludert": false
                    },
                    {
                      "id": 1815,
                      "navn": "HelseIArbeid",
                      "inkludert": false
                    },
                    {
                      "id": 1810,
                      "navn": "Utvikle arbeidsmiljøet",
                      "inkludert": true
                    }
                  ]
                }
              ]
            }
            """.trimIndent()

        val json = Json {
            ignoreUnknownKeys = true
        }

        val plan = json.decodeFromString<PlanKafkamelding>(melding)

        plan.id shouldBe "09c9248f-76d1-40e9-9399-975a452b6e9a"
    }
}
