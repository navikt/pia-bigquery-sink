package no.nav.pia.bigquery.sink.datadefenisjoner

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.bigquery.DatasetId
import io.kotest.matchers.maps.shouldContain
import io.kotest.matchers.shouldBe
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.ia_sak_hendelser_v1
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

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
        val payload = ObjectMapper().readTree(
            """{
             "orgnr": "123456789",
             "navn": "Fiktivia",
             "timestamp": "${LocalDateTime.now()}",
             "status": "VI_BISTÅR"
            }""".trimMargin()
        )

        val content = ia_sak_hendelser_v1.transform(payload).content
        content.shouldContain("tidsstempel" to "AUTO")
        content.shouldContain("orgnr" to "123456789")
        content.shouldContain("navn" to "Fiktivia")
        content.shouldContain("status" to "VI_BISTÅR")
    }
}
