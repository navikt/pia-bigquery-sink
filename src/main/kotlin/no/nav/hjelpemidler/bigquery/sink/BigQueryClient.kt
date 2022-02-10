package no.nav.hjelpemidler.bigquery.sink

import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo
import mu.KotlinLogging
import mu.withLoggingContext
import no.nav.hjelpemidler.bigquery.sink.BigQueryClient.BigQueryClientException

interface BigQueryClient {
    fun datasetPresent(datasetId: DatasetId): Boolean
    fun tablePresent(tableId: TableId): Boolean
    fun create(tableInfo: TableInfo): TableInfo
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

    override fun datasetPresent(datasetId: DatasetId): Boolean = withLoggingContext {
        bigQuery.getDataset(datasetId) != null
    }

    override fun tablePresent(tableId: TableId): Boolean = withLoggingContext {
        bigQuery.getTable(tableId) != null
    }

    override fun create(tableInfo: TableInfo): TableInfo = withLoggingContext {
        val createdTable = bigQuery.create(tableInfo)
        log.info { "Opprettet tabell: '${createdTable.tableId.table}'" }
        createdTable
    }

    override fun insert(tableId: TableId, row: RowToInsert) = withLoggingContext {
        val tableName = tableId.table
        val table = requireNotNull(bigQuery.getTable(tableId)) {
            "Mangler tabell: '$tableName' i BigQuery"
        }
        val rows = listOf(row)
        log.debug {
            "Setter inn rader i tabell: '$tableName', rader: '$rows'"
        }
        val response = table.insert(rows)
        when {
            response.hasErrors() -> throw BigQueryClientException(
                "Lagring i BigQuery feilet: '${response.insertErrors}'"
            )
            else -> log.info { "Rader ble lagret i tabell: '$tableName'" }
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

    override fun insert(tableId: TableId, row: RowToInsert) {
        log.info { "insert(tableId, row) called with tableId: '$tableId', row: '$row'" }
    }

    companion object {
        private val log = KotlinLogging.logger {}
    }
}
