#!/usr/bin/env bash
# Create the ONE topic this harness uses: `events`, 3 partitions, RF 1.
#
# We create it explicitly instead of letting the producer auto-create it,
# because auto-created topics default to 1 partition — which would defeat the
# entire point (you can't observe partition assignment, rebalance, or per-
# partition lag with a single partition).
#
# This runs kafka-topics.sh INSIDE the broker container, so it talks to the
# broker over the internal listener (localhost:9092 from the container's view).
set -euo pipefail

TOPIC="events"
PARTITIONS=3
REPLICATION=1

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION"

echo "---"
echo "Topic '$TOPIC' now looks like:"
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic "$TOPIC"
