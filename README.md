# Kafka learning harness

A tiny, local Kafka setup whose only job is to let you **observe five Kafka
behaviors by hand**. One broker (KRaft, no Zookeeper), one topic (`events`,
3 partitions), a producer, a consumer, and kafka-ui to watch it all.

This is not a service. It has no notification logic, no schema registry, no
Avro, no transactions. On purpose.

---

## What's here

| File | Purpose |
|---|---|
| `docker-compose.yml` | Kafka (`apache/kafka`, KRaft) + kafka-ui. Listener config is commented in detail. |
| `create-topic.sh` | Creates `events` with **3 partitions**, RF 1. Run once. |
| `reset-offsets.sh` | Rewinds a consumer group's offsets to 0 → full replay. |
| `producer.js` | Emits `{userId, seq, ts}` keyed by `userId`. `--count`, `--rate`. |
| `consumer.js` | Reads as a group member. `--group`, `--name`, `--sleep`. |

**The concepts you'll see:** partition key, offset, consumer group, rebalance, lag.

---

## One-time setup

```bash
# 1. Start the broker + kafka-ui
docker compose up -d

# 2. Install the one Node dependency (kafkajs)
npm install

# 3. Create the `events` topic with 3 partitions (do NOT skip — auto-create
#    would give you 1 partition and defeat the whole demo)
./create-topic.sh
```

Open **kafka-ui at http://localhost:8080**. Under cluster `local` → Topics →
`events` you should see **3 partitions**. Keep this tab open — you'll watch
offsets and consumer-group lag here throughout.

Host scripts connect to the broker at `localhost:9094`; kafka-ui connects
internally at `kafka:9092`. (Why two addresses: see the comments at the top of
`docker-compose.yml`.)

When you're completely done: `docker compose down -v`.

---

## The five experiments

Run these **in order**. Each lists the exact commands, then **Expect** (what you
should see) and the **Concept** it demonstrates. Run them yourself — nothing
below runs automatically.

### 1. Baseline — keys map to partitions, offsets order within a partition

```bash
# terminal 1: start a consumer in group A
node consumer.js --group A --name c1

# terminal 2: produce 20 messages
node producer.js --count 20 --rate 5
```

**Expect:** the producer prints `partition=` per send, and a given `userId`
(e.g. `u3`) always prints the **same** partition every time; the consumer prints
`partition/offset/userId/seq` and, within any one partition, offsets increase
0,1,2,… in order. In kafka-ui the 3 partitions each show a nonzero end offset.
**Concept:** partition key → deterministic partitioning, and per-partition
ordering via offsets.

### 2. Replay — restart resumes, offset-reset re-reads everything

```bash
# Stop consumer c1 from experiment 1 (Ctrl-C in terminal 1).

# Restart it — note it does NOT re-read the 20 messages:
node consumer.js --group A --name c1
#   (prints lifecycle/assignment, then sits idle — group A's committed
#    offsets are already at the end. Ctrl-C to stop it again.)

# Now rewind group A to the beginning. The group must have NO active member,
# so make sure c1 is stopped first, then:
./reset-offsets.sh A

# Start the consumer again — this time it replays all 20 from offset 0:
node consumer.js --group A --name c1
```

**Expect:** the plain restart reads nothing new (committed offsets = a bookmark
at the end); after `reset-offsets.sh A`, the next start re-reads all 20 messages
from `offset=0`. **Concept:** committed offsets vs. offset reset —
**this replay is the thing SQS cannot do** (a consumed SQS message is gone; the
Kafka log stays and the offset is just a movable bookmark). Stop c1 when done.

### 3. Fan-out — a second group reads everything independently

```bash
# terminal 1: keep group A running (or start it)
node consumer.js --group A --name c1

# terminal 2: start an INDEPENDENT group B
node consumer.js --group B --name b1

# terminal 3: produce more messages
node producer.js --count 20 --rate 5
```

**Expect:** both `c1` (group A) and `b1` (group B) print **every** message.
B receiving all messages does not depend on A, and each group has its own
offsets in kafka-ui (Consumers tab shows `A` and `B` separately).
**Concept:** consumer groups give independent fan-out — every group gets its
own copy of the stream and its own offsets.

### 4. Scaling + rebalance — partitions split, then re-consolidate

```bash
# terminal 1: first member of group A
node consumer.js --group A --name c1

# terminal 2: SECOND member of the SAME group A
node consumer.js --group A --name c2
#   -> watch BOTH terminals log REBALANCING then GROUP_JOIN with assigned
#      partitions. The 3 partitions split across the two (e.g. c1 gets 2, c2
#      gets 1).

# terminal 3: produce so you can see who reads what
node producer.js --count 100 --rate 5

# Now kill c2 (Ctrl-C in terminal 2) and watch c1 rebalance.
```

**Expect:** starting c2 triggers a `REBALANCING` → `GROUP_JOIN` in both, and the
3 partitions divide between c1 and c2 (each message read by exactly one of
them). Killing c2 triggers another rebalance and **c1 takes over all 3
partitions**. kafka-ui's Consumers tab shows the membership change.
**Concept:** consumer-group scaling and rebalance — partitions are redistributed
as members join and leave.

### 5. Lag — throttled consumer falls behind, then drains

```bash
# terminal 1: a deliberately SLOW consumer (800ms per message)
node consumer.js --group A --name slow --sleep 800

# terminal 2: fire a fast burst — far faster than the consumer can keep up
node producer.js --count 1000 --rate 500
```

Watch kafka-ui → Consumers → group `A`: the **lag** column climbs while the slow
consumer crawls through the backlog. Then:

```bash
# Stop the slow consumer (Ctrl-C in terminal 1) and restart it with NO throttle:
node consumer.js --group A --name fast
```

**Expect:** with `--sleep 800` and `--rate 500`, lag (produced offset minus
committed offset) climbs into the hundreds in kafka-ui; after you remove the
throttle, the consumer races ahead and lag drains back toward 0.
**Concept:** consumer lag — the gap between the log's end and the group's
committed offset, and how throughput determines whether it grows or shrinks.


### For the run with multiple brokers and replicas

docker compose -f docker-compose.replication.yml up -d
docker compose -f docker-compose.replication.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic events

docker compose -f docker-compose.replication.yml stop kafka-2

docker compose -f docker-compose.replication.yml stop kafka-1

docker compose -f docker-compose.replication.yml start kafka-2 kafka-1

docker compose -f docker-compose.replication.yml down -v
---

## Definition of done

All scripts run; kafka-ui shows `events` with 3 partitions and live
consumer-group lag; the five experiments above each behave as written. That's
the whole harness — it stops here by design.
