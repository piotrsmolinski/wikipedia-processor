FROM openjdk:8-jdk-slim

RUN useradd appuser

RUN mkdir -p /app/state  && chown appuser /app/state
VOLUME /app/state

COPY maven/lib /app/lib

# The /app/config directory is a perfect place to update the config by volume mounting
COPY config /app/config

WORKDIR /app

USER appuser

CMD ["java","-cp","lib/*","dev.psmolinski.kafka.wikipedia.WikipediaApplication"]
