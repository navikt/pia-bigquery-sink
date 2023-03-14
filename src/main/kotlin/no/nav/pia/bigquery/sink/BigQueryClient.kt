package no.nav.pia.bigquery.sink

import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.Table
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo
import mu.KotlinLogging
import mu.withLoggingContext
import no.nav.pia.bigquery.sink.BigQueryClient.BigQueryClientException

interface BigQueryClient {
    /**
     * Sjekk om datasett finnes
     */
    fun datasetPresent(datasetId: DatasetId): Boolean

    /**
     * Sjekk om tabell finnes
     */
    fun tablePresent(tableId: TableId): Boolean

    /**
     * Opprett tabell i BigQuery
     */
    fun create(tableInfo: TableInfo): TableInfo

    /**
     * Slett tabell i BigQuery
     */
    fun delete(tableInfo: TableInfo)

    /**
     * Oppdater tabell i BigQuery
     */
    fun update(tableId: TableId, updatedTableInfo: TableInfo): Boolean

    /**
     * Sett in rad i BigQuery-tabell
     */
    fun insert(tableId: TableId, row: RowToInsert)

    class BigQueryClientException(message: String) : RuntimeException(message)
}

class DefaultBigQueryClient(private val projectId: String) : BigQueryClient {
    private val bigQuery = BigQueryOptions.newBuilder()
        .setProjectId(projectId)
        .build()
        .service

    private fun <T> withLoggingContext(block: () -> T) = withLoggingContext(
        "projectId" to projectId,
    ) { block() }

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

    override fun delete(tableInfo: TableInfo) = withLoggingContext {
        bigQuery.delete(tableInfo.tableId)
        log.info { "Slettet tabell: '${tableInfo.tableId.table}'" }
    }

    override fun update(
        tableId: TableId,
        updatedTableInfo: TableInfo,
    ): Boolean = withLoggingContext {
        val table = getTable(tableId)
        when (TableInfo.of(tableId, table.getDefinition())) {
            updatedTableInfo -> {
                log.info { "Skjema for tabell: ${tableId.table} er uendret, oppdaterer ikke tabell i BigQuery" }
                false
            }
            else -> {
                log.info { "Skjema for tabell: ${tableId.table} er endret, oppdaterer tabell i BigQuery" }
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
        val response = table.insert(rows)
        when {
            response.hasErrors() -> throw BigQueryClientException(
                "Lagring i BigQuery feilet: '${response.insertErrors}'"
            )
            else -> log.debug { "${rows.size} rader ble lagret i tabell: '${tableId.table}'" }
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
    override fun delete(tableInfo: TableInfo) {
        log.info { "delete(tableInfo) called with tableInfo: '$tableInfo'" }
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
