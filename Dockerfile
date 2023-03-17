FROM gcr.io/distroless/java17-debian11:latest
ENV TZ="Europe/Oslo"
ENV JAVA_TOOL_OPTIONS="-XX:+UseParallelGC -XX:MaxRAMPercentage=75"
COPY build/libs/pia-bigquery-sink-all.jar /app/app.jar
WORKDIR /app
CMD ["app.jar"]
