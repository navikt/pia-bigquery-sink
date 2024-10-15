# pia-bigquery-sink
En app for å ta Kafkameldinger og lagre de i BigQuery.

Den har enkel migrering som kan legge til manglende tabeller og kollonner, 
men den kan ikke endre kollonner eller legge til nye som "required".

## Teknologier
* Kafka
* Kotlin
* Ktor
* BigQuery

## Legge til nytt view i BigQuery

1. Opprett nytt schema i `/datadefinisjoner/` 
   - schemaId må stemme med navn på topic
   - alle felt i første versjon må være `required()`
   - anbefales å teste transformasjon fra jsonstring til dataklasse
2. Opprett nytt topic i f.eks. lydia-api
   - navn på topic må stemme med schemaId
   - skriv navn på topic i nais-dev.yaml og nais-prod.yaml
3. Legg til topic i kafkaKonfig i App.kt i `pia-bigquery-sink`

## Kontakt
Team Pia

## Kode generert av GitHub Copilot

Dette repoet tar i bruk GitHub Copilot for kodeforslag.