package no.nav.hjelpemidler.bigquery.sink

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.DatasetId
import mu.KotlinLogging
import mu.withLoggingContext

interface BigQueryClient {
    fun migrate()
    fun ping(): Boolean
}

class DefaultBigQueryClient(
    private val datasetId: DatasetId,
    private val schemaRegistry: SchemaRegistry,
) : BigQueryClient {
    private val bigQuery: BigQuery = BigQueryOptions.newBuilder()
        .setProjectId(datasetId.project)
        .build()
        .service

    private fun <T> withLoggingContext(block: () -> T) = withLoggingContext(
        "projectId" to datasetId.project,
        "datasetId" to datasetId.dataset
    ) { block() }

    override fun migrate() = withLoggingContext {
        schemaRegistry
            .mapValues { it.value.toTableInfo(datasetId) }
            .forEach { (_, tableInfo) ->
                val existingTable = bigQuery.getTable(tableInfo.tableId)
                if (existingTable == null) {
                    val createdTable = bigQuery.create(tableInfo)
                    log.info { "Opprettet tabell: '${createdTable.tableId.table}'" }
                }
            }
    }

    override fun ping(): Boolean = withLoggingContext {
        when (bigQuery.getDataset(datasetId)) {
            null -> log.error { "Fikk ikke kontakt med BigQuery" }.let { false }
            else -> log.info { "Fikk kontakt med BigQuery" }.let { true }
        }
    }

    class BigQueryClientException(message: String) : RuntimeException(message)

    companion object {
        private val log = KotlinLogging.logger {}
    }
}

class LocalBigQueryClient : BigQueryClient {
    override fun migrate() =
        log.info { "migrate() called" }

    override fun ping(): Boolean {
        log.info { "ping() called" }
        return true
    }

    companion object {
        private val log = KotlinLogging.logger {}
    }
}
