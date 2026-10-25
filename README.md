# credit-queue

Atomic Redis Lua credit gate and write-behind event queue with two-phase reservations (`reserve`, `settle`, `release`) and monotonic versioning.

Built for usage-based APIs, AI/LLM token metering, and credit-gated SaaS plans where you need:
- **Fast admission control** (~2ms single Redis `EVAL`) that prevents concurrent requests from racing past a budget.
- **Two-phase holds** (`reserve` before work, `settle` actual cost or `release` on failure).
- **Atomic write-behind queueing** so every balance change pushes an ordered event (`version`) for background persistence to Postgres/ClickHouse.
- **Zero runtime dependencies** — works with `@upstash/redis` or any Redis client that implements `eval`, `hgetall`, and `lpop`.

---

## Installation

```bash
pnpm add credit-queue
# or
npm install credit-queue
```

---

## How It Works

```
Caller (API / MCP / AI Route)
  │
  ▼
CreditQueue.reserve / settle / release
  │
  ├─► Runs a single atomic Redis Lua script:
  │     1. Checks or updates `{prefix}:{key}:state` (Hash)
  │        • budget, consumed, reserved, available, version
  │        • active per-request holds (`res:{requestId}`)
  │     2. Increments `version` (monotonic integer: v + 1)
  │     3. Appends `CreditQueueEvent` JSON to `{prefix}:events:queue` (List)
  │
  └─► Cold-cache miss (`uninitialized`)?
        • Calls optional `loadBalance(key)` from your DB
        • Seeds `{prefix}:{key}:state` (`SEED_BALANCE_LUA`)
        • Retries the mutation automatically
```

---

## Quick Start

```ts
import { Redis } from '@upstash/redis';
import { CreditQueue } from 'credit-queue';

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL!,
  token: process.env.UPSTASH_REDIS_REST_TOKEN!,
});

type BillingMetadata = {
  teamId: string;
  workspaceId?: string;
  userId?: string;
  apiKeyId?: string;
  toolName?: string;
};

export const creditQueue = new CreditQueue<BillingMetadata>({
  redis,
  prefix: 'billing', // defaults to 'credit-queue'
  stateTtlSeconds: 35 * 24 * 60 * 60, // 35 days
  loadBalance: async (key) => {
    // Called only on a cold Redis cache miss for `key`
    const period = await db.billingPeriod.findUnique({ where: { id: key } });
    if (!period) return null;

    return {
      key: period.id,
      budget: period.allowance + period.granted,
      consumed: period.consumed,
      version: period.checkpointVersion,
    };
  },
});
```

### 1. Two-phase flow (`reserve` → `settle` / `release`)

Use this when the exact cost isn't known upfront (e.g. LLM streaming or paginated searches) or when you want to hold credits while an operation is in flight:

```ts
const requestId = crypto.randomUUID();

// 1. Reserve estimated max cost before executing work
const reservation = await creditQueue.reserve({
  key: billingPeriodId,
  requestId,
  amount: 5_000,
  metadata: {
    teamId: 'team_123',
    userId: 'user_456',
    toolName: 'searchBrands',
  },
});

if (!reservation.ok) {
  // reservation.status === 'insufficient_credits' | 'uninitialized' | 'invalid_state'
  return new Response('Out of credits', { status: 402 });
}

try {
  const result = await runExpensiveOperation();

  // 2a. Settle the actual cost (releases the 5,000 hold and adds 3,200 to `consumed`)
  await creditQueue.settle({
    key: billingPeriodId,
    requestId,
    amount: 3_200,
    metadata: {
      teamId: 'team_123',
      userId: 'user_456',
      toolName: 'searchBrands',
    },
  });

  return Response.json(result);
} catch (err) {
  // 2b. Operation failed — release the 5,000 hold without charging `consumed`
  await creditQueue.release({
    key: billingPeriodId,
    requestId,
    metadata: {
      teamId: 'team_123',
      userId: 'user_456',
      toolName: 'searchBrands',
    },
  });
  throw err;
}
```

### 2. Direct settlement (`settle` without prior `reserve`)

If you already know the exact cost or want to record post-hoc usage directly:

```ts
const settled = await creditQueue.settle({
  key: billingPeriodId,
  requestId: crypto.randomUUID(),
  amount: 1_000,
  metadata: {
    teamId: 'team_123',
    toolName: 'getMedia',
  },
});
```

### 3. Draining the queue to your database

A background job or scheduled worker pops batches from Redis and persists them to your database, using `version` to guard against out-of-order batch writes:

```ts
const events = await creditQueue.dequeue(100);

for (const event of events) {
  if (event.operation === 'settle') {
    await db.usageEvent.insertIfNotExists({
      id: event.requestId,
      periodId: event.key,
      amount: event.amount,
      metadata: event.metadata,
      createdAt: event.createdAt,
    });
  }

  // Only advance the database checkpoint if this event is newer
  await db.sql`
    UPDATE billing_period
    SET consumed = ${event.consumed},
        checkpoint_version = ${event.version}
    WHERE id = ${event.key}
      AND checkpoint_version < ${event.version}
  `;
}
```

---

## API Reference

### `new CreditQueue<TMetadata>(config)`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `redis` | `CreditQueueRedisClient` | *required* | Redis client implementing `eval`, `hgetall`, and `lpop` (e.g. `@upstash/redis`). |
| `prefix` | `string` | `'credit-queue'` | Key prefix for Redis state hashes and the default event queue key. |
| `queueKey` | `string` | `'{prefix}:events:queue'` | Custom Redis list key for the write-behind queue. |
| `stateTtlSeconds` | `number` | `3024000` (35 days) | Expiration TTL in seconds applied to state hashes. |
| `loadBalance` | `(key: string) => Promise<SeedBalanceInput \| null>` | `undefined` | Optional callback to load initial state from your primary database when a key is not in Redis. |

### Methods

- **`queue.reserve(input)`** — Holds `amount` in `reserved` if `available >= amount` (or if `allowOverdraft: true`), increments `version`, and enqueues a `'reserve'` event.
- **`queue.settle(input)`** — Releases any active hold for `requestId`, adds `amount` to `consumed`, increments `version`, and enqueues a `'settle'` event.
- **`queue.release(input)`** — Releases the active hold for `requestId` without adding to `consumed`, increments `version`, and enqueues a `'release'` event. Returns `status: 'no_active_reservation'` if no hold exists.
- **`queue.seedBalance(params)`** — Initializes or updates `{prefix}:{key}:state`. Always updates `budget`; only overwrites `consumed` and `version` if the incoming `version` is greater than the current Redis `version`.
- **`queue.getState(key)`** — Reads the current `CreditBalanceState` (`budget`, `consumed`, `reserved`, `available`, `version`) or returns `null` if uninitialized.
- **`queue.dequeue(count = 100)`** — Atomically pops up to `count` `CreditQueueEvent` items from the queue via `LPOP`.

---

## Redis Data Layout

`credit-queue` uses only **two Redis keys**:

1. **`{prefix}:{key}:state` (Hash)**
   - `key`: Bucket identifier (e.g. billing period ID)
   - `budget`: Total integer budget for the period
   - `consumed`: Total settled usage
   - `reserved`: Total currently held across in-flight requests
   - `version`: Monotonically increasing integer counter
   - `res:{requestId}`: Held amount for each active in-flight reservation (removed on `settle` or `release`)

2. **`{prefix}:events:queue` (List)**
   - FIFO queue of JSON-encoded `CreditQueueEvent` payloads appended via `RPUSH` and drained via `LPOP`.

---

## License

MIT
