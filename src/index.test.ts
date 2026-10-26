import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  CreditQueue,
  type CreditQueueEvent,
  MUTATE_CREDITS_AND_ENQUEUE_LUA,
  SEED_BALANCE_LUA,
} from './index';

class FakeRedis {
  hashes = new Map<string, Map<string, string>>();
  lists = new Map<string, string[]>();
  ttls = new Map<string, number>();

  exists(key: string): number {
    return this.hashes.has(key) || this.lists.has(key) ? 1 : 0;
  }

  hget(key: string, field: string): string | null {
    return this.hashes.get(key)?.get(field) ?? null;
  }

  hexists(key: string, field: string): number {
    return this.hashes.get(key)?.has(field) ? 1 : 0;
  }

  hset(key: string, ...pairs: Array<string | number>): void {
    let hash = this.hashes.get(key);
    if (!hash) {
      hash = new Map();
      this.hashes.set(key, hash);
    }
    for (let i = 0; i < pairs.length; i += 2) {
      const field = String(pairs[i]);
      const value = String(pairs[i + 1]);
      hash.set(field, value);
    }
  }

  hdel(key: string, field: string): void {
    this.hashes.get(key)?.delete(field);
  }

  async hgetall<TData extends Record<string, unknown> = Record<string, unknown>>(
    key: string,
  ): Promise<TData | null> {
    const hash = this.hashes.get(key);
    if (!hash || hash.size === 0) return null;
    const out: Record<string, string> = {};
    for (const [k, v] of hash.entries()) {
      out[k] = v;
    }
    return out as unknown as TData;
  }

  rpush(key: string, value: string): void {
    const list = this.lists.get(key) ?? [];
    list.push(value);
    this.lists.set(key, list);
  }

  async lpop<TData = unknown>(key: string, count = 1): Promise<TData | null> {
    const list = this.lists.get(key);
    if (!list || list.length === 0) return null;
    return list.splice(0, count) as unknown as TData;
  }

  llen(key: string): number {
    return this.lists.get(key)?.length ?? 0;
  }

  async eval<TArgs extends unknown[], TData = unknown>(
    script: string,
    keys: string[],
    args: TArgs,
  ): Promise<TData> {
    const argv = args as Array<string | number>;
    if (script === SEED_BALANCE_LUA) {
      return this.runSeedScript(keys, argv) as unknown as TData;
    }
    if (script === MUTATE_CREDITS_AND_ENQUEUE_LUA) {
      return this.runMutateScript(keys, argv) as unknown as TData;
    }
    throw new Error('Unexpected Lua script');
  }

  runSeedScript(keys: string[], argv: Array<string | number>): string {
    const stateKey = keys[0]!;
    const key = String(argv[0]);
    const budget = Number(argv[1] ?? 0);
    const consumed = Number(argv[2] ?? 0);
    const ttlSeconds = Number(argv[3] ?? 3024000);

    if (this.exists(stateKey) === 0) {
      this.hset(
        stateKey,
        'key',
        key,
        'budget',
        budget,
        'consumed',
        consumed,
        'reserved',
        0,
      );
      this.ttls.set(stateKey, ttlSeconds);
    } else {
      this.hset(stateKey, 'key', key, 'budget', budget);
      if (!this.ttls.has(stateKey)) {
        this.ttls.set(stateKey, ttlSeconds);
      }
    }

    const finalBudget = Number(this.hget(stateKey, 'budget') ?? 0);
    const finalConsumed = Number(this.hget(stateKey, 'consumed') ?? 0);
    const finalReserved = Number(this.hget(stateKey, 'reserved') ?? 0);

    return JSON.stringify({
      key,
      budget: finalBudget,
      consumed: finalConsumed,
      reserved: finalReserved,
      available: finalBudget - (finalConsumed + finalReserved),
    });
  }

  runMutateScript(keys: string[], argv: Array<string | number>): string {
    const [stateKey, queueKey] = keys as [string, string];

    const operation = String(argv[0]) as 'reserve' | 'settle' | 'release';
    const amount = Number(argv[1]);
    const key = String(argv[2]);
    const requestId = String(argv[3]);
    const createdAt = String(argv[4]);
    const stateTtlSeconds = Number(argv[5] ?? 3024000);
    const allowOverdraft = String(argv[6]) === '1';
    const metadataJson = String(argv[7] ?? '');

    if (
      !Number.isFinite(amount) ||
      amount < 0 ||
      Math.floor(amount) !== amount
    ) {
      return JSON.stringify({
        ok: false,
        status: 'invalid_state',
        reason: 'invalid_amount',
        state: null,
        event: null,
      });
    }

    if (this.exists(stateKey) === 0) {
      return JSON.stringify({
        ok: false,
        status: 'uninitialized',
        state: null,
        event: null,
      });
    }

    const budget = Number(this.hget(stateKey, 'budget') ?? 0);
    let consumed = Number(this.hget(stateKey, 'consumed') ?? 0);
    let reserved = Number(this.hget(stateKey, 'reserved') ?? 0);

    const buildState = () => ({
      key,
      budget,
      consumed,
      reserved,
      available: budget - (consumed + reserved),
    });

    const resField = `res:${requestId}`;

    if (operation === 'reserve') {
      if (this.hexists(stateKey, resField) === 1) {
        return JSON.stringify({
          ok: false,
          status: 'invalid_state',
          reason: 'reservation_already_exists',
          state: buildState(),
          event: null,
        });
      }

      const available = budget - (consumed + reserved);
      if (!allowOverdraft && available < amount) {
        return JSON.stringify({
          ok: false,
          status: 'insufficient_credits',
          state: buildState(),
          event: null,
        });
      }

      reserved += amount;
      this.hset(stateKey, 'reserved', reserved, resField, amount);
      if (!this.ttls.has(stateKey)) {
        this.ttls.set(stateKey, stateTtlSeconds);
      }

      return JSON.stringify({
        ok: true,
        status: 'applied',
        state: buildState(),
        event: null,
      });
    }

    if (operation === 'release') {
      if (this.hexists(stateKey, resField) === 0) {
        return JSON.stringify({
          ok: true,
          status: 'no_active_reservation',
          state: buildState(),
          event: null,
        });
      }

      const reservedAmount = Number(this.hget(stateKey, resField) ?? 0);
      let releaseAmount = reservedAmount;
      if (amount > 0 && amount < reservedAmount) {
        releaseAmount = amount;
      }
      reserved = Math.max(0, reserved - releaseAmount);

      this.hdel(stateKey, resField);
      this.hset(stateKey, 'reserved', reserved);
      if (!this.ttls.has(stateKey)) {
        this.ttls.set(stateKey, stateTtlSeconds);
      }

      return JSON.stringify({
        ok: true,
        status: 'applied',
        state: buildState(),
        event: null,
      });
    }

    let reservedAmount = 0;
    if (this.hexists(stateKey, resField) === 1) {
      reservedAmount = Number(this.hget(stateKey, resField) ?? 0);
      reserved = Math.max(0, reserved - reservedAmount);
      this.hdel(stateKey, resField);
    }

    consumed += amount;

    this.hset(stateKey, 'consumed', consumed, 'reserved', reserved);
    if (!this.ttls.has(stateKey)) {
      this.ttls.set(stateKey, stateTtlSeconds);
    }

    const nextState = buildState();
    const event: CreditQueueEvent = {
      key,
      requestId,
      amount,
      reservedAmount,
      budget,
      consumed,
      reserved,
      available: nextState.available,
      createdAt,
      ...(metadataJson ? { metadata: JSON.parse(metadataJson) } : {}),
    };

    this.rpush(queueKey, JSON.stringify(event));

    return JSON.stringify({
      ok: true,
      status: 'applied',
      state: nextState,
      event,
    });
  }
}

const BUCKET_KEY = 'period-001';

let fakeRedis: FakeRedis;
let loadBalanceMock: ReturnType<typeof vi.fn>;
let queue: CreditQueue;

beforeEach(() => {
  fakeRedis = new FakeRedis();
  loadBalanceMock = vi.fn().mockResolvedValue(null);
  queue = new CreditQueue({
    redis: fakeRedis,
    loadBalance: loadBalanceMock,
  });
});

describe('CreditQueue', () => {
  it('seeds a balance and reads it back without overwriting live consumed on re-seed', async () => {
    const seeded = await queue.seedBalance({
      key: BUCKET_KEY,
      budget: 5_500_000,
      consumed: 100_000,
    });

    expect(seeded).toEqual({
      key: BUCKET_KEY,
      budget: 5_500_000,
      consumed: 100_000,
      reserved: 0,
      available: 5_400_000,
    });

    const reseeded = await queue.seedBalance({
      key: BUCKET_KEY,
      budget: 10_000_000,
      consumed: 0,
    });
    expect(reseeded).toEqual({
      key: BUCKET_KEY,
      budget: 10_000_000,
      consumed: 100_000,
      reserved: 0,
      available: 9_900_000,
    });

    const readBack = await queue.getState(BUCKET_KEY);
    expect(readBack).toEqual(reseeded);
  });

  it('reserves and releases in Redis, and only enqueues events on settle', async () => {
    await queue.seedBalance({
      key: BUCKET_KEY,
      budget: 12_000,
      consumed: 0,
    });

    const reserve1 = await queue.reserve({
      key: BUCKET_KEY,
      requestId: 'req-1',
      amount: 5_000,
    });

    expect(reserve1.ok).toBe(true);
    expect(reserve1.event).toBeNull();
    expect(reserve1.state).toMatchObject({
      consumed: 0,
      reserved: 5_000,
      available: 7_000,
    });

    const reserve2 = await queue.reserve({
      key: BUCKET_KEY,
      requestId: 'req-2',
      amount: 4_000,
    });

    expect(reserve2.ok).toBe(true);
    expect(reserve2.event).toBeNull();
    expect(reserve2.state).toMatchObject({
      consumed: 0,
      reserved: 9_000,
      available: 3_000,
    });

    const settle1 = await queue.settle({
      key: BUCKET_KEY,
      requestId: 'req-1',
      amount: 3_000,
      metadata: {
        billingTeamId: 'team-1',
        workspaceId: 'ws-1',
        userId: 'user-1',
        toolName: 'searchBrands',
      },
    });

    expect(settle1.ok).toBe(true);
    expect(settle1.state).toMatchObject({
      consumed: 3_000,
      reserved: 4_000,
      available: 5_000,
    });
    expect(settle1.event).toMatchObject({
      key: BUCKET_KEY,
      requestId: 'req-1',
      amount: 3_000,
      reservedAmount: 5_000,
      metadata: {
        billingTeamId: 'team-1',
        workspaceId: 'ws-1',
        userId: 'user-1',
        toolName: 'searchBrands',
      },
    });

    const release2 = await queue.release({
      key: BUCKET_KEY,
      requestId: 'req-2',
    });

    expect(release2.ok).toBe(true);
    expect(release2.event).toBeNull();
    expect(release2.state).toMatchObject({
      consumed: 3_000,
      reserved: 0,
      available: 9_000,
    });

    expect(fakeRedis.llen(queue.queueKey)).toBe(1);
    const events = await queue.dequeue(10);
    expect(events.map((e) => [e.requestId, e.amount, e.consumed])).toEqual([
      ['req-1', 3_000, 3_000],
    ]);
    expect(fakeRedis.llen(queue.queueKey)).toBe(0);
  });

  it('denies reserve when available balance is insufficient without queuing an event', async () => {
    await queue.seedBalance({
      key: BUCKET_KEY,
      budget: 5_000,
      consumed: 4_000,
    });

    const denied = await queue.reserve({
      key: BUCKET_KEY,
      requestId: 'req-deny',
      amount: 2_000,
    });

    expect(denied.ok).toBe(false);
    expect(denied.status).toBe('insufficient_credits');
    expect(denied.event).toBeNull();
    expect(denied.state).toMatchObject({
      available: 1_000,
      consumed: 4_000,
      reserved: 0,
    });
    expect(fakeRedis.llen(queue.queueKey)).toBe(0);
  });

  it('auto-seeds via loadBalance callback on cold Redis cache miss', async () => {
    loadBalanceMock.mockResolvedValue({
      key: BUCKET_KEY,
      budget: 21_000_000,
      consumed: 5_000_000,
    });

    const result = await queue.settle({
      key: BUCKET_KEY,
      requestId: 'req-cold-1',
      amount: 50_000,
    });

    expect(loadBalanceMock).toHaveBeenCalledWith(BUCKET_KEY);
    expect(result.ok).toBe(true);
    expect(result.status).toBe('applied');
    expect(result.state).toEqual({
      key: BUCKET_KEY,
      budget: 21_000_000,
      consumed: 5_050_000,
      reserved: 0,
      available: 15_950_000,
    });
    expect(fakeRedis.llen(queue.queueKey)).toBe(1);
  });

  it('invokes onEnqueue only when settle enqueues an event', async () => {
    const onEnqueueMock = vi.fn();
    const hookQueue = new CreditQueue({
      redis: fakeRedis,
      onEnqueue: onEnqueueMock,
    });

    await hookQueue.seedBalance({
      key: BUCKET_KEY,
      budget: 10_000,
    });

    await hookQueue.reserve({
      key: BUCKET_KEY,
      requestId: 'req-hook-1',
      amount: 2_000,
    });
    expect(onEnqueueMock).not.toHaveBeenCalled();

    await hookQueue.settle({
      key: BUCKET_KEY,
      requestId: 'req-hook-1',
      amount: 1_500,
    });
    expect(onEnqueueMock).toHaveBeenCalledTimes(1);
    expect(onEnqueueMock).toHaveBeenCalledWith(
      expect.objectContaining({
        event: expect.objectContaining({
          key: BUCKET_KEY,
          requestId: 'req-hook-1',
          amount: 1_500,
          reservedAmount: 2_000,
        }),
        state: expect.objectContaining({
          key: BUCKET_KEY,
          consumed: 1_500,
          reserved: 0,
          available: 8_500,
        }),
      }),
    );
  });
});
