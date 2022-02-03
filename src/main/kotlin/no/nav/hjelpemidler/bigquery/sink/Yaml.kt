package no.nav.hjelpemidler.bigquery.sink

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule

val yamlMapper: YAMLMapper = YAMLMapper.builder()
    .addModule(kotlinModule())
    .build()
