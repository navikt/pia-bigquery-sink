FROM ghcr.io/navikt/baseimages/temurin:17
COPY build/libs/hm-bigquery-sink-all.jar app.jar
