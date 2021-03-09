FROM openjdk:8-jdk-slim

VOLUME /app/state

COPY maven/lib /app/lib

# The /app/config directory is a perfect place to update the config by volume mounting
COPY config /app/config

WORKDIR /app

RUN useradd appuser
USER appuser

CMD ["java","-cp","lib/*","dev.psmolinski.kafka.wikipedia.WikipediaApplication"]
