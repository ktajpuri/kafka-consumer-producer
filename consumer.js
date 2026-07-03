// consumer.js — read from `events` as a member of a consumer group, printing
// enough detail to watch partition assignment, offsets, and rebalances by hand.
//
// Run examples:
//   node consumer.js --group A --name c1
//   node consumer.js --group A --name c2            # second member of group A
//   node consumer.js --group B --name b1            # independent group (fan-out)
//   node consumer.js --group A --name slow --sleep 800   # throttle to build lag
//
// Comments here are teaching material.

const { Kafka } = require('kafkajs');

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i !== -1 && process.argv[i + 1] !== undefined ? process.argv[i + 1] : fallback;
}

const GROUP = arg('group', 'A');     // CONSUMER GROUP id
const NAME = arg('name', 'c1');      // human label so you can tell instances apart
const SLEEP = parseInt(arg('sleep', '0'), 10); // artificial per-message delay (ms)

// A CONSUMER GROUP is Kafka's unit of load-sharing AND of independent reads:
//  - Within one group, the topic's partitions are divided among the members,
//    so each partition is read by exactly ONE member (that's scaling).
//  - Across DIFFERENT groups, every group gets its own copy of every message
//    and its own offsets (that's fan-out). Group A's progress never affects B.
const kafka = new Kafka({ clientId: `consumer-${NAME}`, brokers: ['localhost:9094'] });
const consumer = kafka.consumer({ groupId: GROUP });

async function main() {
  await consumer.connect();

  // ---- Make consumer-group lifecycle VISIBLE ----
  // These events are half the point of the demo. A REBALANCE is Kafka
  // reassigning partitions among the group's members — it happens whenever a
  // member joins or leaves. We log the raw events so you can watch it happen.
  const { GROUP_JOIN, REBALANCING, CRASH } = consumer.events;

  consumer.on(GROUP_JOIN, (e) => {
    // After a (re)join, the payload tells us which partitions THIS member now
    // owns. With 3 partitions and 1 consumer you'll see all three; with 2
    // consumers you'll see the 3 split (e.g. 2 + 1).
    const assignment = e.payload.memberAssignment.events || [];
    console.log(
      `[${NAME}/${GROUP}] GROUP_JOIN — assigned partitions: [${assignment.join(', ')}]`
    );
  });

  consumer.on(REBALANCING, () => {
    // Fires when the group is reshuffling — e.g. right after you start a second
    // consumer, or kill one. Old assignments are effectively revoked here.
    console.log(`[${NAME}/${GROUP}] REBALANCING — partitions being reassigned...`);
  });

  consumer.on(CRASH, (e) => {
    console.log(`[${NAME}/${GROUP}] CRASH — ${e.payload.error}`);
  });

  // fromBeginning only matters the FIRST time a group has no committed offset.
  // Once the group has committed offsets, Kafka resumes from those instead
  // (that's why restarting a consumer doesn't re-read — see experiment #2).
  await consumer.subscribe({ topic: 'events', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const { userId, seq } = JSON.parse(message.value.toString());
      // OFFSET is the message's position within its partition — a
      // monotonically increasing id. The group commits "next offset to read"
      // per partition; that bookmark is what lets it resume or replay.
      console.log(
        `[${NAME}/${GROUP}] partition=${partition} offset=${message.offset} ` +
          `userId=${userId} seq=${seq}`
      );

      // Optional throttle. Slower processing than the producer's send rate is
      // exactly how you make LAG (unread backlog) grow on purpose.
      if (SLEEP > 0) await new Promise((r) => setTimeout(r, SLEEP));
    },
  });

  console.log(`[${NAME}/${GROUP}] running. Ctrl-C to stop (triggers a rebalance).`);
}

// Disconnect cleanly on Ctrl-C so the member LEAVES the group promptly, which
// makes the survivor's rebalance in experiment #4 happen immediately.
const shutdown = async () => {
  console.log(`\n[${NAME}/${GROUP}] leaving group...`);
  try {
    await consumer.disconnect();
  } finally {
    process.exit(0);
  }
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().catch((err) => {
  console.error(`[${NAME}/${GROUP}] error:`, err);
  process.exit(1);
});
