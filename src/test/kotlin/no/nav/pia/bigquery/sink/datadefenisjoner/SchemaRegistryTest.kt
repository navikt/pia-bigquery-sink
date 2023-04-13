package no.nav.pia.bigquery.sink.datadefenisjoner

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.bigquery.DatasetId
import io.kotest.matchers.maps.shouldContain
import io.kotest.matchers.shouldBe
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`ia-sak-leveranse-v1`
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`ia-sak-statistikk-v1`
import no.nav.pia.bigquery.sink.oversettFraCetTilUtc
import org.junit.jupiter.api.Test
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
             
            }""".trimMargin()
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
            """{
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
            "muligeDagsverk":6314.79015997463,
            "sykefraversprosent":14.6662125361897,
            "arstall":2022,
            "kvartal":4,
            "tapteDagsverkSiste4Kvartal":1000.0,
            "muligeDagsverkSiste4Kvartal":9434.0,
            "sykefraversprosentSiste4Kvartal":10.4836642887593,
            "kvartaler":[{"årstall":2021,"kvartal":2},{"årstall":2021,"kvartal":3},{"årstall":2021,"kvartal":4},{"årstall":2022,"kvartal":1}],
            "sektor":"PRIVAT",
            "neringer":[{"navn":"Utleie av egen eller leid fast eiendom ellers","kode":"68.209"}],
            "bransjeprogram":null,
            "postnummer":"6240",
            "kommunenummer":"1507",
            "fylkesnummer":"15"
            }""".trimIndent()
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
        content.shouldContain("tidsstempel" to "AUTO")
    }

    @Test
    internal fun `real payload with null values transformed to ia_sak_hendelser_v1 row`() {
        val payload = ObjectMapper().readTree(
            """{
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
                "muligeDagsverk":null,
                "sykefraversprosent":null,
                "arstall":null,
                "kvartal":null,
                "tapteDagsverkSiste4Kvartal":null,
                "muligeDagsverkSiste4Kvartal":null,
                "sykefraversprosentSiste4Kvartal":null,
                "kvartaler":[],
                "sektor":"STATLIG",
                "neringer":[{"navn":"Alminnelige somatiske sykehus","kode":"86.101"}],
                "bransjeprogram":"SYKEHUS",
                "postnummer":"5021",
                "kommunenummer":"4601",
                "fylkesnummer":"46"}""".trimIndent()
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
    internal fun `kan transformere ia-sak-leveranse melding` () {
        val json = ObjectMapper().readTree("""
            {
                "id":1,
                "saksnummer":"01GVJFE0REVM09011RS6B11X46",
                "modul":{
                    "id":2,
                    "navn":"Videreutvikle sykefraværsrutiner",
                    "iaTjeneste":{
                        "id":3,
                        "navn":"Redusere sykefravær"
                    }
                },
                "frist":"2023-03-15",
                "status":"UNDER_ARBEID",
                "opprettetAv":"X12345",
                "sistEndret":"2023-03-15T12:10:39.369468",
                "sistEndretAv":"X12345",
                "sistEndretAvRolle":"SAKSBEHANDLER",
                "fullført":null
            }
        """.trimIndent())

        val content = `ia-sak-leveranse-v1`.transform(json).content
        content.shouldContain("id" to 1)
        content.shouldContain("saksnummer" to "01GVJFE0REVM09011RS6B11X46")
        content.shouldContain("iaModulId" to 2)
        content.shouldContain("iaModulNavn" to "Videreutvikle sykefraværsrutiner")
        content.shouldContain("iaTjenesteId" to 3)
        content.shouldContain("iaTjenesteNavn" to "Redusere sykefravær")
        content.shouldContain("frist" to "2023-03-15")
        content.shouldContain("status" to "UNDER_ARBEID")
        content.shouldContain("opprettetAv" to "X12345")
        content.shouldContain("sistEndret" to "2023-03-15T11:10:39.369468")
        content.shouldContain("sistEndretAv" to "X12345")
        content.shouldContain("sistEndretAvRolle" to "SAKSBEHANDLER")
        content.shouldContain("fullført" to null)
    }
}
