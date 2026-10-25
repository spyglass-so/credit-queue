# redledger

Atomic Redis Lua credit ledger with two-phase reservations (`reserve`, `settle`, `release`), monotonic versioning, and an atomic write-behind event queue.

Works with `@upstash/redis` or any Redis client providing `eval`, `hgetall`, and `lpop`.

## Install

```bash
pnpm add redledger
```

## Quick Start

```ts
import { Redis } from '@upstash/redis';
import { RedLedger } from 'redledger';

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL!,
  token: process.env.UPSTASH_REDIS_REST_TOKEN!,
});

export const creditLedger = new RedLedger({
  redis,
  prefix: 'billing',
  loadBalance: async (key) => {
    const row = await db.getBillingPeriodByPk(key);
    if (!row) return null;
    return {
      key: row.id,
      budget: row.allowanceMicrodollars + row.grantedMicrodollars,
      consumed: row.consumedMicrodollars,
      version: row.checkpointVersion,
    };
  },
});

// 1. Reserve credits before running an operation
const reserved = await creditLedger.reserve({
  key: 'period_1',
  requestId: 'req_123',
  amount: 5_000,
  metadata: {
    billingTeamId: 'team_1',
    workspaceId: 'ws_1',
    userId: 'user_1',
    apiKeyId: 'key_1',
  },
});

if (!reserved.ok) {
  throw new Error(`Denied: ${reserved.status}`);
}

// 2. Settle actual usage (or release on failure)
await creditLedger.settle({
  key: 'period_1',
  requestId: 'req_123',
  amount: 3_200,
  metadata: {
    billingTeamId: 'team_1',
    workspaceId: 'ws_1',
    userId: 'user_1',
    apiKeyId: 'key_1',
  },
});

// 3. Drain events in background worker to checkpoint Postgres
const events = await creditLedger.dequeue(100);
```

## Redis Keys

- `{prefix}:{key}:state` (Hash) — Stores `budget`, `consumed`, `reserved`, `version`, and active holds (`res:{requestId}`).
- `{prefix}:events:queue` (List) — Write-behind queue where every mutation atomically `RPUSH`es a `CreditLedgerEvent`.
