#!/usr/bin/env bash
# Reset a consumer group's committed offsets on `events` back to 0.
#
# A consumer group remembers, per partition, the offset of the next message it
# should read (its "committed offset"). Rewinding those offsets to 0 (the
# earliest message) makes the group re-read the whole topic from the beginning
# the next time a consumer in that group starts — a full REPLAY.
#
# This is the thing a queue like SQS fundamentally cannot do: in SQS a consumed
# message is gone. In Kafka the log stays put and the offset is just a bookmark,
# so you can move the bookmark back and read it all again.
#
# The group must have NO active members while you do this (Kafka refuses to
# reset offsets for a group that's currently being consumed), so stop the
# consumer first.
#
# Usage: ./reset-offsets.sh <groupId>
set -euo pipefail

GROUP="${1:-}"
if [[ -z "$GROUP" ]]; then
  echo "Usage: ./reset-offsets.sh <groupId>" >&2
  exit 1
fi

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group "$GROUP" \
  --topic events \
  --reset-offsets \
  --to-earliest \
  --execute

echo "---"
echo "Offsets for group '$GROUP' after reset:"
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group "$GROUP" \
  --describe
