package no.nav.pia.bigquery.sink.datadefenisjoner

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.bigquery.DatasetId
import io.kotest.matchers.maps.shouldContain
import io.kotest.matchers.shouldBe
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`ia-sak-statistikk-v1`
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
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
        val now = LocalDateTime.now()
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
        content.shouldContain("opprettetTidspunkt" to "${now.truncatedTo(ChronoUnit.MICROS)}")
        content.shouldContain("endretTidspunkt" to "${now.truncatedTo(ChronoUnit.MICROS)}")
        content.shouldContain("sykefraversprosent" to null)
        content.shouldContain("tidsstempel" to "AUTO")
    }

    @Test
    internal fun `real payload transformed to ia_sak_hendelser_v1 row`() {
        val payload = ObjectMapper().readTree(
            """{
            "saksnummer":"01GVDFDVA8DKN811GBQT0W065J",
            "orgnr":"972385778",
            "eierAvSak":null,
            "status":"NY",
            "endretAvHendelseId":"01GVDFDVA8DKN811GBQT0W065J",
            "hendelse":"OPPRETT_SAK_FOR_VIRKSOMHET",
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
        content.shouldContain("orgnr" to "972385778")
        content.shouldContain("eierAvSak" to null)
        content.shouldContain("endretAvHendelseId" to "01GVDFDVA8DKN811GBQT0W065J")
        content.shouldContain("status" to "NY")
        content.shouldContain("opprettetTidspunkt" to "2023-03-13T13:34:21.128356")
        content.shouldContain("endretTidspunkt" to "2023-03-13T13:34:21.128356")
        content.shouldContain("tidsstempel" to "AUTO")
    }
}
