package no.nav.hjelpemidler.bigquery.sink.schema

import com.google.cloud.bigquery.DatasetId

class Registry(val datasetId: DatasetId, val registry: Map<SchemaDefinition.Id, SchemaDefinition>) :
    Map<SchemaDefinition.Id, SchemaDefinition> by registry
