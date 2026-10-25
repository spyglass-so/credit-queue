import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  type CreditLedgerEvent,
  MUTATE_CREDITS_AND_ENQUEUE_LUA,
  RedLedger,
  SEED_BILLING_PERIOD_LUA,
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
    if (script === SEED_BILLING_PERIOD_LUA) {
      return this.runSeedScript(keys, argv) as unknown as TData;
    }
    if (script === MUTATE_CREDITS_AND_ENQUEUE_LUA) {
      return this.runMutateScript(keys, argv) as unknown as TData;
    }
    throw new Error('Unexpected Lua script');
  }

  runSeedScript(keys: string[], argv: Array<string | number>): string {
    const stateKey = keys[0]!;
    const billingPeriodId = String(argv[0]);
    const billingTeamId = String(argv[1]);
    const budget = Number(argv[2] ?? 0);
    const consumed = Number(argv[3] ?? 0);
    const checkpointVersion = Number(argv[4] ?? 0);
    const ttlSeconds = Number(argv[5] ?? 3024000);

    if (this.exists(stateKey) === 0) {
      this.hset(
        stateKey,
        'billingPeriodId',
        billingPeriodId,
        'billingTeamId',
        billingTeamId,
        'budgetMicrodollars',
        budget,
        'consumedMicrodollars',
        consumed,
        'reservedMicrodollars',
        0,
        'version',
        checkpointVersion,
      );
      this.ttls.set(stateKey, ttlSeconds);
    } else {
      const currentVersion = Number(this.hget(stateKey, 'version') ?? 0);
      this.hset(
        stateKey,
        'billingPeriodId',
        billingPeriodId,
        'billingTeamId',
        billingTeamId,
        'budgetMicrodollars',
        budget,
      );
      if (checkpointVersion > currentVersion) {
        this.hset(
          stateKey,
          'consumedMicrodollars',
          consumed,
          'version',
          checkpointVersion,
        );
      }
      if (!this.ttls.has(stateKey)) {
        this.ttls.set(stateKey, ttlSeconds);
      }
    }

    const finalBudget = Number(
      this.hget(stateKey, 'budgetMicrodollars') ?? 0,
    );
    const finalConsumed = Number(
      this.hget(stateKey, 'consumedMicrodollars') ?? 0,
    );
    const finalReserved = Number(
      this.hget(stateKey, 'reservedMicrodollars') ?? 0,
    );
    const finalVersion = Number(this.hget(stateKey, 'version') ?? 0);
    const finalTeamId = this.hget(stateKey, 'billingTeamId') ?? billingTeamId;

    return JSON.stringify({
      billingPeriodId,
      billingTeamId: finalTeamId,
      budgetMicrodollars: finalBudget,
      consumedMicrodollars: finalConsumed,
      reservedMicrodollars: finalReserved,
      availableMicrodollars: finalBudget - (finalConsumed + finalReserved),
      version: finalVersion,
    });
  }

  runMutateScript(keys: string[], argv: Array<string | number>): string {
    const [stateKey, queueKey] = keys as [string, string];

    const operation = String(argv[0]) as 'reserve' | 'settle' | 'release';
    const amount = Number(argv[1]);
    const eventId = String(argv[2]);
    const requestId = String(argv[3]);
    let billingTeamId = String(argv[4]);
    const billingPeriodId = String(argv[5]);
    const workspaceId = String(argv[6]);
    const userId = String(argv[7]);
    const userToTeamId = String(argv[8]);
    const apiKeyId = String(argv[9]);
    const createdAt = String(argv[10]);
    const stateTtlSeconds = Number(argv[11] ?? 3024000);
    const allowOverdraft = String(argv[12]) === '1';
    const metadataJson = String(argv[13] ?? '');

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

    const budget = Number(this.hget(stateKey, 'budgetMicrodollars') ?? 0);
    let consumed = Number(this.hget(stateKey, 'consumedMicrodollars') ?? 0);
    let reserved = Number(this.hget(stateKey, 'reservedMicrodollars') ?? 0);
    let version = Number(this.hget(stateKey, 'version') ?? 0);
    const storedBillingTeamId = this.hget(stateKey, 'billingTeamId');
    if (storedBillingTeamId) {
      billingTeamId = storedBillingTeamId;
    }

    const buildState = () => ({
      billingPeriodId,
      billingTeamId,
      budgetMicrodollars: budget,
      consumedMicrodollars: consumed,
      reservedMicrodollars: reserved,
      availableMicrodollars: budget - (consumed + reserved),
      version,
    });

    const resField = `res:${requestId}`;
    let reservedAmount = 0;
    let eventAmount = amount;

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
      reservedAmount = amount;
      version += 1;

      this.hset(
        stateKey,
        'reservedMicrodollars',
        reserved,
        'version',
        version,
        resField,
        amount,
      );
    } else if (operation === 'settle') {
      if (this.hexists(stateKey, resField) === 1) {
        reservedAmount = Number(this.hget(stateKey, resField) ?? 0);
        reserved = Math.max(0, reserved - reservedAmount);
        this.hdel(stateKey, resField);
      }

      consumed += amount;
      version += 1;

      this.hset(
        stateKey,
        'consumedMicrodollars',
        consumed,
        'reservedMicrodollars',
        reserved,
        'version',
        version,
      );
    } else if (operation === 'release') {
      if (this.hexists(stateKey, resField) === 0) {
        return JSON.stringify({
          ok: true,
          status: 'no_active_reservation',
          state: buildState(),
          event: null,
        });
      }

      reservedAmount = Number(this.hget(stateKey, resField) ?? 0);
      let releaseAmount = reservedAmount;
      if (amount > 0 && amount < reservedAmount) {
        releaseAmount = amount;
      }
      eventAmount = releaseAmount;
      reserved = Math.max(0, reserved - releaseAmount);
      version += 1;

      this.hdel(stateKey, resField);
      this.hset(
        stateKey,
        'reservedMicrodollars',
        reserved,
        'version',
        version,
      );
    }

    if (!this.ttls.has(stateKey)) {
      this.ttls.set(stateKey, stateTtlSeconds);
    }

    const nextState = buildState();
    const event: CreditLedgerEvent = {
      eventId,
      requestId,
      billingTeamId,
      billingPeriodId,
      workspaceId,
      userId: userId !== '' ? userId : null,
      userToTeamId: userToTeamId !== '' ? userToTeamId : null,
      apiKeyId: apiKeyId !== '' ? apiKeyId : null,
      operation,
      amount: eventAmount,
      reservedAmount,
      version,
      budgetMicrodollars: budget,
      consumedMicrodollars: consumed,
      reservedMicrodollars: reserved,
      availableMicrodollars: nextState.availableMicrodollars,
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

const PERIOD_ID = 'period-001';
const BILLING_TEAM_ID = 'team-billing-001';
const WORKSPACE_ID = 'team-workspace-001';
const USER_ID = 'user-001';
const UTT_ID = 'utt-001';
const API_KEY_ID = 'apikey-001';

let fakeRedis: FakeRedis;
let loadPeriodMock: ReturnType<typeof vi.fn>;
let ledger: RedLedger;

beforeEach(() => {
  fakeRedis = new FakeRedis();
  loadPeriodMock = vi.fn().mockResolvedValue(null);
  ledger = new RedLedger({
    redis: fakeRedis,
    loadPeriod: loadPeriodMock,
  });
});

describe('RedLedger', () => {
  it('seeds a billing period balance and reads it back', async () => {
    const seeded = await ledger.seedPeriod({
      billingPeriodId: PERIOD_ID,
      billingTeamId: BILLING_TEAM_ID,
      budgetMicrodollars: 5_500_000,
      consumedMicrodollars: 100_000,
      checkpointVersion: 4,
    });

    expect(seeded).toEqual({
      billingPeriodId: PERIOD_ID,
      billingTeamId: BILLING_TEAM_ID,
      budgetMicrodollars: 5_500_000,
      consumedMicrodollars: 100_000,
      reservedMicrodollars: 0,
      availableMicrodollars: 5_400_000,
      version: 4,
    });

    const readBack = await ledger.getState(PERIOD_ID);
    expect(readBack).toEqual(seeded);
  });

  it('reserves, settles, and releases atomically while incrementing period version and queuing events', async () => {
    await ledger.seedPeriod({
      billingPeriodId: PERIOD_ID,
      billingTeamId: BILLING_TEAM_ID,
      budgetMicrodollars: 12_000,
      consumedMicrodollars: 0,
      checkpointVersion: 0,
    });

    const reserve1 = await ledger.reserve({
      eventId: 'evt-1',
      requestId: 'req-1',
      billingTeamId: BILLING_TEAM_ID,
      billingPeriodId: PERIOD_ID,
      workspaceId: WORKSPACE_ID,
      userId: USER_ID,
      userToTeamId: UTT_ID,
      apiKeyId: API_KEY_ID,
      amount: 5_000,
      metadata: { toolName: 'searchBrands' },
    });

    expect(reserve1.ok).toBe(true);
    expect(reserve1.status).toBe('applied');
    expect(reserve1.state).toMatchObject({
      consumedMicrodollars: 0,
      reservedMicrodollars: 5_000,
      availableMicrodollars: 7_000,
      version: 1,
    });

    const reserve2 = await ledger.reserve({
      eventId: 'evt-2',
      requestId: 'req-2',
      billingTeamId: BILLING_TEAM_ID,
      billingPeriodId: PERIOD_ID,
      workspaceId: WORKSPACE_ID,
      userId: USER_ID,
      userToTeamId: UTT_ID,
      apiKeyId: API_KEY_ID,
      amount: 4_000,
    });

    expect(reserve2.ok).toBe(true);
    expect(reserve2.state).toMatchObject({
      consumedMicrodollars: 0,
      reservedMicrodollars: 9_000,
      availableMicrodollars: 3_000,
      version: 2,
    });

    const settle1 = await ledger.settle({
      eventId: 'evt-3',
      requestId: 'req-1',
      billingTeamId: BILLING_TEAM_ID,
      billingPeriodId: PERIOD_ID,
      workspaceId: WORKSPACE_ID,
      userId: USER_ID,
      userToTeamId: UTT_ID,
      apiKeyId: API_KEY_ID,
      amount: 3_000,
    });

    expect(settle1.ok).toBe(true);
    expect(settle1.state).toMatchObject({
      consumedMicrodollars: 3_000,
      reservedMicrodollars: 4_000,
      availableMicrodollars: 5_000,
      version: 3,
    });

    const release2 = await ledger.release({
      eventId: 'evt-4',
      requestId: 'req-2',
      billingTeamId: BILLING_TEAM_ID,
      billingPeriodId: PERIOD_ID,
      workspaceId: WORKSPACE_ID,
      userId: USER_ID,
      userToTeamId: UTT_ID,
      apiKeyId: API_KEY_ID,
    });

    expect(release2.ok).toBe(true);
    expect(release2.state).toMatchObject({
      consumedMicrodollars: 3_000,
      reservedMicrodollars: 0,
      availableMicrodollars: 9_000,
      version: 4,
    });

    expect(fakeRedis.llen(ledger.queueKey)).toBe(4);
    const events = await ledger.dequeue(10);
    expect(events.map((e) => [e.operation, e.version, e.amount])).toEqual([
      ['reserve', 1, 5_000],
      ['reserve', 2, 4_000],
      ['settle', 3, 3_000],
      ['release', 4, 4_000],
    ]);
    expect(fakeRedis.llen(ledger.queueKey)).toBe(0);
  });

  it('denies reserve when available balance is insufficient without changing version or queuing an event', async () => {
    await ledger.seedPeriod({
      billingPeriodId: PERIOD_ID,
      billingTeamId: BILLING_TEAM_ID,
      budgetMicrodollars: 5_000,
      consumedMicrodollars: 4_000,
      checkpointVersion: 7,
    });

    const denied = await ledger.reserve({
      eventId: 'evt-deny',
      requestId: 'req-deny',
      billingTeamId: BILLING_TEAM_ID,
      billingPeriodId: PERIOD_ID,
      workspaceId: WORKSPACE_ID,
      amount: 2_000,
    });

    expect(denied.ok).toBe(false);
    expect(denied.status).toBe('insufficient_credits');
    expect(denied.event).toBeNull();
    expect(denied.state).toMatchObject({
      availableMicrodollars: 1_000,
      consumedMicrodollars: 4_000,
      reservedMicrodollars: 0,
      version: 7,
    });
    expect(fakeRedis.llen(ledger.queueKey)).toBe(0);
  });

  it('auto-seeds via loadPeriod callback on cold Redis cache miss', async () => {
    loadPeriodMock.mockResolvedValue({
      billingPeriodId: PERIOD_ID,
      billingTeamId: BILLING_TEAM_ID,
      budgetMicrodollars: 21_000_000,
      consumedMicrodollars: 5_000_000,
      checkpointVersion: 12,
    });

    const result = await ledger.settle({
      eventId: 'evt-cold-1',
      requestId: 'req-cold-1',
      billingTeamId: BILLING_TEAM_ID,
      billingPeriodId: PERIOD_ID,
      workspaceId: WORKSPACE_ID,
      userId: USER_ID,
      userToTeamId: UTT_ID,
      amount: 50_000,
    });

    expect(loadPeriodMock).toHaveBeenCalledWith(PERIOD_ID);
    expect(result.ok).toBe(true);
    expect(result.status).toBe('applied');
    expect(result.state).toEqual({
      billingPeriodId: PERIOD_ID,
      billingTeamId: BILLING_TEAM_ID,
      budgetMicrodollars: 21_000_000,
      consumedMicrodollars: 5_050_000,
      reservedMicrodollars: 0,
      availableMicrodollars: 15_950_000,
      version: 13,
    });
    expect(fakeRedis.llen(ledger.queueKey)).toBe(1);
  });
});
