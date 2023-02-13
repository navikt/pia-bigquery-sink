FROM ghcr.io/navikt/baseimages/temurin:17
COPY build/libs/pia-bigquery-sink-all.jar app.jar
