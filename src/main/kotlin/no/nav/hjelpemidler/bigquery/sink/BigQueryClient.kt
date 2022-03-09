package no.nav.hjelpemidler.bigquery.sink

import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.Table
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo
import mu.KotlinLogging
import mu.withLoggingContext
import no.nav.hjelpemidler.bigquery.sink.BigQueryClient.BigQueryClientException

interface BigQueryClient {
    fun datasetPresent(datasetId: DatasetId): Boolean
    fun tablePresent(tableId: TableId): Boolean
    fun create(tableInfo: TableInfo): TableInfo
    fun update(tableId: TableId, updatedTableInfo: TableInfo): Boolean
    fun insert(tableId: TableId, row: RowToInsert)

    class BigQueryClientException(message: String) : RuntimeException(message)
}

class DefaultBigQueryClient(private val datasetId: DatasetId) : BigQueryClient {
    private val bigQuery = BigQueryOptions.newBuilder()
        .setProjectId(datasetId.project)
        .build()
        .service

    private fun <T> withLoggingContext(block: () -> T) = withLoggingContext(
        "projectId" to datasetId.project,
        "datasetId" to datasetId.dataset
    ) { block() }

    private val TableInfo.schema: Schema
        get() = requireNotNull(getDefinition<TableDefinition>().schema) {
            "Tabell: '${tableId.table}' mangler skjema"
        }

    private val FieldList.names: Set<String> get() = map { it.name }.toSet()

    private fun getTable(tableId: TableId): Table = requireNotNull(bigQuery.getTable(tableId)) {
        "Mangler tabell: '${tableId.table}' i BigQuery"
    }

    override fun datasetPresent(datasetId: DatasetId): Boolean = withLoggingContext {
        val present = bigQuery.getDataset(datasetId) != null
        log.info { "dataset: $datasetId, present: $present" }
        present
    }

    override fun tablePresent(tableId: TableId): Boolean = withLoggingContext {
        val present = bigQuery.getTable(tableId) != null
        log.info { "table: $tableId, present: $present" }
        present
    }

    override fun create(tableInfo: TableInfo): TableInfo = withLoggingContext {
        val createdTable = bigQuery.create(tableInfo)
        log.info { "Opprettet tabell: '${createdTable.tableId.table}'" }
        createdTable
    }

    override fun update(
        tableId: TableId,
        updatedTableInfo: TableInfo,
    ): Boolean = withLoggingContext {
        val table = getTable(tableId)
        val existingFields = table.schema.fields.names
        val updatedFields = updatedTableInfo.schema.fields.names
        val newFields = updatedFields.minus(existingFields)
        when {
            newFields.isEmpty() -> {
                log.info { "Ingen nye kolonner, ingen oppdatering av tabell: ${tableId.table} nødvendig" }
                false
            }
            else -> {
                log.info { "Legger til nye kolonner i tabell: ${tableId.table}, kolonner: $newFields" }
                val updatedTable = table.toBuilder()
                    .setDescription(updatedTableInfo.description)
                    .setDefinition(updatedTableInfo.getDefinition())
                    .build()
                updatedTable.update()
                true
            }
        }
    }

    override fun insert(tableId: TableId, row: RowToInsert) = withLoggingContext {
        val table = getTable(tableId)
        val rows = listOf(row)
        log.debug {
            "Setter inn rader i tabell: '${tableId.table}', rader: '$rows'"
        }
        val response = table.insert(rows)
        when {
            response.hasErrors() -> throw BigQueryClientException(
                "Lagring i BigQuery feilet: '${response.insertErrors}'"
            )
            else -> log.debug { "Rader ble lagret i tabell: '${tableId.table}'" }
        }
    }

    companion object {
        private val log = KotlinLogging.logger {}
    }
}

class LocalBigQueryClient : BigQueryClient {
    override fun datasetPresent(datasetId: DatasetId): Boolean {
        log.info { "datasetPresent(datasetId) called with datasetId: '$datasetId'" }
        return true
    }

    override fun tablePresent(tableId: TableId): Boolean {
        log.info { "tablePresent(tableId) called with tableId: '$tableId'" }
        return true
    }

    override fun create(tableInfo: TableInfo): TableInfo {
        log.info { "create(tableInfo) called with tableInfo: '$tableInfo'" }
        return tableInfo
    }

    override fun update(tableId: TableId, updatedTableInfo: TableInfo): Boolean {
        log.info { "update(tableId, updatedTableInfo) called with tableId: '$tableId', updatedTableInfo: $updatedTableInfo" }
        return true
    }

    override fun insert(tableId: TableId, row: RowToInsert) {
        log.info { "insert(tableId, row) called with tableId: '$tableId', row: '$row'" }
    }

    companion object {
        private val log = KotlinLogging.logger {}
    }
}
