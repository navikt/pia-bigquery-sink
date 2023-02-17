package no.nav.pia.bigquery.sink.konfigurasjon

enum class Clusters(val clusterId: String) {
    PROD_GCP("prod-gcp"), DEV_GCP("dev-gcp"), LOKAL("local")
}

object Miljø {
    val cluster = System.getenv("NAIS_CLUSTER_NAME")
    val team_project_id = getEnvVar("TEAM_PROJECT_ID")
}
