package no.nav.pia.bigquery.sink.schema

import com.google.cloud.bigquery.DatasetId
import io.kotest.assertions.assertSoftly
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class SchemaDefinitionTest {
    @Test
    internal fun `should create valid SchemaId from String`() {
        val schemaId = SchemaDefinition.Id.of("foobar-v1")
        assertSoftly {
            schemaId.name shouldBe "foobar"
            schemaId.version shouldBe 1
        }
    }

    @Test
    internal fun `should create valid SchemaId from String 2`() {
        val schemaId = SchemaDefinition.Id.of("ia-sak-v1")
        assertSoftly {
            schemaId.name shouldBe "ia-sak"
            schemaId.version shouldBe 1
        }
    }

    @Test
    internal fun `should create valid TableId from SchemaId`() {
        val tableName = "foobar-v1"
        val datasetId = DatasetId.of("foo", "bar")
        val schemaId = SchemaDefinition.Id.of(tableName)
        val tableId = schemaId.toTableId(datasetId)
        assertSoftly {
            tableId.project shouldBe datasetId.project
            tableId.dataset shouldBe datasetId.dataset
            tableId.table shouldBe tableName
        }
    }
}
