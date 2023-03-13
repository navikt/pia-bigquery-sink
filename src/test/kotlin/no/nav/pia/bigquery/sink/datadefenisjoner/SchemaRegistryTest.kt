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
             "endretTidspunkt": "$now"
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
        content.shouldContain("tidsstempel" to "AUTO")
    }
}
