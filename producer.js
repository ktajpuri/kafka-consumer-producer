// producer.js — emit keyed messages to `events` so you can watch how Kafka
// maps keys to partitions and preserves per-key ordering.
//
// Run examples:
//   node producer.js                       # 20 messages, ~5/sec
//   node producer.js --count 20 --rate 5
//   node producer.js --count 1000 --rate 500   # the lag experiment
//
// Comments here are teaching material — Kafka concepts are explained at the
// point they show up.

const { Kafka } = require('kafkajs');

// ---- tiny CLI parser: reads `--flag value` pairs ----
function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i !== -1 && process.argv[i + 1] !== undefined ? process.argv[i + 1] : fallback;
}

const COUNT = parseInt(arg('count', '20'), 10); // how many messages total
const RATE = parseInt(arg('rate', '5'), 10);    // messages per second (throttle)

// Five stable user IDs. A message's KEY decides its partition: Kafka hashes the
// key and takes it modulo the partition count. Because the key set is fixed and
// the partition count is fixed, each userId deterministically lands on the SAME
// partition every time — which is how Kafka gives you ordering *per key*.
const USER_IDS = ['u1', 'u2', 'u3', 'u4', 'u5'];

// Connect to the EXTERNAL listener advertised as localhost:9094 (see compose).
const kafka = new Kafka({ clientId: 'producer', brokers: ['localhost:9094'] });
const producer = kafka.producer();

async function main() {
  await producer.connect();
  console.log(`Producing ${COUNT} messages at ~${RATE}/sec to topic "events"...`);

  const delayMs = 1000 / RATE; // spacing between sends to hit the target rate

  for (let seq = 0; seq < COUNT; seq++) {
    // Cycle through the users so keys spread across partitions predictably.
    const userId = USER_IDS[seq % USER_IDS.length];
    const payload = { userId, seq, ts: Date.now() };

    // Sending with `key: userId` is what triggers Kafka's key-based
    // partitioning. `messages[0].partition` in the result tells us which
    // partition the broker actually wrote to.
    const [meta] = await producer.send({
      topic: 'events',
      messages: [{ key: userId, value: JSON.stringify(payload) }],
    });

    // Log userId, seq, and the ASSIGNED PARTITION. Watch: the same userId
    // always prints the same partition number.
    console.log(`sent  userId=${userId}  seq=${seq}  -> partition=${meta.partition}`);

    if (seq < COUNT - 1) await new Promise((r) => setTimeout(r, delayMs));
  }

  await producer.disconnect();
  console.log('Done.');
}

main().catch((err) => {
  console.error('Producer error:', err);
  process.exit(1);
});
