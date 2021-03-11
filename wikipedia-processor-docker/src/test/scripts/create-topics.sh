#!/usr/bin/env bash

kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic wikipedia.parsed \
  --replication-factor 1 \
  --partitions 4

kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --topic wikipedia.parsed.count-by-domain \
  --replication-factor 1 \
  --partitions 4 \
  --config cleanup.policy=compact
