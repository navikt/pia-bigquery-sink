package no.nav.pia.bigquery.sink.konfigurasjon

enum class Clusters(
    val clusterId: String,
) {
    PROD_GCP("prod-gcp"),
    DEV_GCP("dev-gcp"),
    LOKAL("local"),
}

object NaisEnvironment {
    val cluster = getEnvVar("NAIS_CLUSTER_NAME")
    val team_project_id = getEnvVar("GCP_TEAM_PROJECT_ID")
}
