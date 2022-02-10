package no.nav.hjelpemidler.bigquery.sink.schema

import com.google.cloud.bigquery.DatasetId
import io.kotest.assertions.assertSoftly
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class SchemaIdTest {

    @Test
    internal fun `should create valid SchemaId from String`() {
        val schemaId = SchemaId.of("foobar_v1")
        assertSoftly {
            schemaId.name shouldBe "foobar"
            schemaId.version shouldBe 1
        }
    }

    @Test
    internal fun `should create valid TableId from SchemaId`() {
        val tableName = "foobar_v1"
        val datasetId = DatasetId.of("foo", "bar")
        val schemaId = SchemaId.of(tableName)
        val tableId = schemaId.toTableId(datasetId)
        assertSoftly {
            tableId.project shouldBe datasetId.project
            tableId.dataset shouldBe datasetId.dataset
            tableId.table shouldBe tableName
        }
    }
}
