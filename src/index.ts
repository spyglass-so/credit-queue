import { randomUUID } from 'node:crypto';

export type CreditOperation = 'reserve' | 'settle' | 'release';

export type CreditLedgerEvent<
  TMetadata extends Record<string, unknown> = Record<string, unknown>,
> = {
  eventId: string;
  periodId: string;
  requestId: string;
  operation: CreditOperation;
  amount: number;
  reservedAmount: number;
  budget: number;
  consumed: number;
  reserved: number;
  available: number;
  version: number;
  createdAt: string;
  metadata?: TMetadata;
};

export type PeriodBalanceState = {
  periodId: string;
  budget: number;
  consumed: number;
  reserved: number;
  available: number;
  version: number;
};

export type CreditMutationStatus =
  | 'applied'
  | 'insufficient_credits'
  | 'uninitialized'
  | 'no_active_reservation'
  | 'invalid_state';

export type CreditMutationResult<
  TMetadata extends Record<string, unknown> = Record<string, unknown>,
> = {
  ok: boolean;
  status: CreditMutationStatus;
  reason?: string;
  state: PeriodBalanceState | null;
  event: CreditLedgerEvent<TMetadata> | null;
};

export type CreditMutationInput<
  TMetadata extends Record<string, unknown> = Record<string, unknown>,
> = {
  eventId?: string;
  periodId: string;
  requestId: string;
  amount: number;
  allowOverdraft?: boolean;
  createdAt?: string;
  metadata?: TMetadata;
};

export type SeedPeriodInput = {
  periodId: string;
  budget: number;
  consumed?: number;
  version?: number;
  ttlSeconds?: number;
};

export interface RedLedgerRedisClient {
  eval<TArgs extends unknown[], TData = unknown>(
    script: string,
    keys: string[],
    args: TArgs,
  ): Promise<TData>;
  hgetall<TData extends Record<string, unknown> = Record<string, unknown>>(
    key: string,
  ): Promise<TData | null>;
  lpop<TData = unknown>(key: string, count?: number): Promise<TData | null>;
}

export type RedLedgerConfig<
  TMetadata extends Record<string, unknown> = Record<string, unknown>,
> = {
  redis: RedLedgerRedisClient;
  prefix?: string;
  queueKey?: string;
  stateTtlSeconds?: number;
  loadPeriod?: (periodId: string) => Promise<SeedPeriodInput | null>;
  __metadataType?: TMetadata;
};

const DEFAULT_PREFIX = 'billing';
const DEFAULT_STATE_TTL_SECONDS = 35 * 24 * 60 * 60;

export const SEED_PERIOD_LUA = `
local stateKey = KEYS[1]
local periodId = ARGV[1]
local budget = tonumber(ARGV[2]) or 0
local consumed = tonumber(ARGV[3]) or 0
local version = tonumber(ARGV[4]) or 0
local ttlSeconds = tonumber(ARGV[5]) or 3024000

if redis.call('EXISTS', stateKey) == 0 then
  redis.call(
    'HSET',
    stateKey,
    'periodId', periodId,
    'budget', budget,
    'consumed', consumed,
    'reserved', 0,
    'version', version
  )
  redis.call('EXPIRE', stateKey, ttlSeconds)
else
  local currentVersion = tonumber(redis.call('HGET', stateKey, 'version') or '0')
  redis.call(
    'HSET',
    stateKey,
    'periodId', periodId,
    'budget', budget
  )
  if version > currentVersion then
    redis.call(
      'HSET',
      stateKey,
      'consumed', consumed,
      'version', version
    )
  end
  if redis.call('TTL', stateKey) == -1 then
    redis.call('EXPIRE', stateKey, ttlSeconds)
  end
end

local finalBudget = tonumber(redis.call('HGET', stateKey, 'budget') or '0')
local finalConsumed = tonumber(redis.call('HGET', stateKey, 'consumed') or '0')
local finalReserved = tonumber(redis.call('HGET', stateKey, 'reserved') or '0')
local finalVersion = tonumber(redis.call('HGET', stateKey, 'version') or '0')

return cjson.encode({
  periodId = periodId,
  budget = finalBudget,
  consumed = finalConsumed,
  reserved = finalReserved,
  available = finalBudget - (finalConsumed + finalReserved),
  version = finalVersion
})
`;

export const MUTATE_CREDITS_AND_ENQUEUE_LUA = `
local stateKey = KEYS[1]
local queueKey = KEYS[2]

local operation = ARGV[1]
local amount = tonumber(ARGV[2])
local eventId = ARGV[3]
local periodId = ARGV[4]
local requestId = ARGV[5]
local createdAt = ARGV[6]
local stateTtlSeconds = tonumber(ARGV[7]) or 3024000
local allowOverdraft = ARGV[8] == '1'
local metadataJson = ARGV[9]

if not amount or amount < 0 or math.floor(amount) ~= amount then
  return cjson.encode({
    ok = false,
    status = 'invalid_state',
    reason = 'invalid_amount',
    state = cjson.null,
    event = cjson.null
  })
end

if redis.call('EXISTS', stateKey) == 0 then
  return cjson.encode({
    ok = false,
    status = 'uninitialized',
    state = cjson.null,
    event = cjson.null
  })
end

local budget = tonumber(redis.call('HGET', stateKey, 'budget') or '0')
local consumed = tonumber(redis.call('HGET', stateKey, 'consumed') or '0')
local reserved = tonumber(redis.call('HGET', stateKey, 'reserved') or '0')
local version = tonumber(redis.call('HGET', stateKey, 'version') or '0')

local function buildState()
  return {
    periodId = periodId,
    budget = budget,
    consumed = consumed,
    reserved = reserved,
    available = budget - (consumed + reserved),
    version = version
  }
end

local resField = 'res:' .. requestId
local reservedAmount = 0
local eventAmount = amount

if operation == 'reserve' then
  if redis.call('HEXISTS', stateKey, resField) == 1 then
    return cjson.encode({
      ok = false,
      status = 'invalid_state',
      reason = 'reservation_already_exists',
      state = buildState(),
      event = cjson.null
    })
  end

  local available = budget - (consumed + reserved)
  if not allowOverdraft and available < amount then
    return cjson.encode({
      ok = false,
      status = 'insufficient_credits',
      state = buildState(),
      event = cjson.null
    })
  end

  reserved = reserved + amount
  reservedAmount = amount
  version = version + 1

  redis.call(
    'HSET',
    stateKey,
    'reserved', reserved,
    'version', version,
    resField, amount
  )
elseif operation == 'settle' then
  if redis.call('HEXISTS', stateKey, resField) == 1 then
    reservedAmount = tonumber(redis.call('HGET', stateKey, resField) or '0')
    reserved = math.max(0, reserved - reservedAmount)
    redis.call('HDEL', stateKey, resField)
  end

  consumed = consumed + amount
  version = version + 1

  redis.call(
    'HSET',
    stateKey,
    'consumed', consumed,
    'reserved', reserved,
    'version', version
  )
elseif operation == 'release' then
  if redis.call('HEXISTS', stateKey, resField) == 0 then
    return cjson.encode({
      ok = true,
      status = 'no_active_reservation',
      state = buildState(),
      event = cjson.null
    })
  end

  reservedAmount = tonumber(redis.call('HGET', stateKey, resField) or '0')
  local releaseAmount = reservedAmount
  if amount > 0 and amount < reservedAmount then
    releaseAmount = amount
  end
  eventAmount = releaseAmount
  reserved = math.max(0, reserved - releaseAmount)
  version = version + 1

  redis.call('HDEL', stateKey, resField)
  redis.call(
    'HSET',
    stateKey,
    'reserved', reserved,
    'version', version
  )
else
  return cjson.encode({
    ok = false,
    status = 'invalid_state',
    reason = 'unknown_operation',
    state = buildState(),
    event = cjson.null
  })
end

if redis.call('TTL', stateKey) == -1 then
  redis.call('EXPIRE', stateKey, stateTtlSeconds)
end

local nextState = buildState()
local event = {
  eventId = eventId,
  periodId = periodId,
  requestId = requestId,
  operation = operation,
  amount = eventAmount,
  reservedAmount = reservedAmount,
  budget = budget,
  consumed = consumed,
  reserved = reserved,
  available = nextState.available,
  version = version,
  createdAt = createdAt
}

if metadataJson and metadataJson ~= '' then
  event.metadata = cjson.decode(metadataJson)
end

redis.call('RPUSH', queueKey, cjson.encode(event))

return cjson.encode({
  ok = true,
  status = 'applied',
  state = nextState,
  event = event
})
`;

export class RedLedger<
  TMetadata extends Record<string, unknown> = Record<string, unknown>,
> {
  private readonly redis: RedLedgerRedisClient;
  private readonly prefix: string;
  public readonly queueKey: string;
  private readonly stateTtlSeconds: number;
  private readonly loadPeriod?: (
    periodId: string,
  ) => Promise<SeedPeriodInput | null>;

  constructor(config: RedLedgerConfig<TMetadata>) {
    this.redis = config.redis;
    this.prefix = config.prefix ?? DEFAULT_PREFIX;
    this.queueKey = config.queueKey ?? `${this.prefix}:credit-events:queue`;
    this.stateTtlSeconds = config.stateTtlSeconds ?? DEFAULT_STATE_TTL_SECONDS;
    this.loadPeriod = config.loadPeriod;
  }

  public stateKey(periodId: string): string {
    return `${this.prefix}:period:${periodId}:state`;
  }

  public async seedPeriod(params: SeedPeriodInput): Promise<PeriodBalanceState> {
    const raw = await this.redis.eval<
      [string, number, number, number, number],
      string | PeriodBalanceState
    >(
      SEED_PERIOD_LUA,
      [this.stateKey(params.periodId)],
      [
        params.periodId,
        Math.trunc(params.budget),
        Math.trunc(params.consumed ?? 0),
        Math.trunc(params.version ?? 0),
        params.ttlSeconds ?? this.stateTtlSeconds,
      ],
    );

    return typeof raw === 'string'
      ? (JSON.parse(raw) as PeriodBalanceState)
      : raw;
  }

  public async getState(periodId: string): Promise<PeriodBalanceState | null> {
    const raw = await this.redis.hgetall<Record<string, string | number>>(
      this.stateKey(periodId),
    );
    if (!raw || Object.keys(raw).length === 0) {
      return null;
    }

    const budget = Number(raw.budget ?? 0);
    const consumed = Number(raw.consumed ?? 0);
    const reserved = Number(raw.reserved ?? 0);
    const version = Number(raw.version ?? 0);

    return {
      periodId: String(raw.periodId ?? periodId),
      budget,
      consumed,
      reserved,
      available: budget - (consumed + reserved),
      version,
    };
  }

  public async mutate(
    operation: CreditOperation,
    input: CreditMutationInput<TMetadata>,
  ): Promise<CreditMutationResult<TMetadata>> {
    if (!Number.isFinite(input.amount) || input.amount < 0) {
      throw new Error(`Invalid credit amount: ${input.amount}`);
    }

    const stateKey = this.stateKey(input.periodId);
    const argv: [
      CreditOperation,
      number,
      string,
      string,
      string,
      string,
      number,
      string,
      string,
    ] = [
      operation,
      Math.trunc(input.amount),
      input.eventId ?? randomUUID(),
      input.periodId,
      input.requestId,
      input.createdAt ?? new Date().toISOString(),
      this.stateTtlSeconds,
      input.allowOverdraft ? '1' : '0',
      input.metadata ? JSON.stringify(input.metadata) : '',
    ];

    const firstRaw = await this.redis.eval<
      typeof argv,
      string | CreditMutationResult<TMetadata>
    >(MUTATE_CREDITS_AND_ENQUEUE_LUA, [stateKey, this.queueKey], argv);
    const firstAttempt =
      typeof firstRaw === 'string'
        ? (JSON.parse(firstRaw) as CreditMutationResult<TMetadata>)
        : firstRaw;

    if (firstAttempt.status !== 'uninitialized' || !this.loadPeriod) {
      return firstAttempt;
    }

    const period = await this.loadPeriod(input.periodId);
    if (!period) {
      return firstAttempt;
    }

    await this.seedPeriod(period);

    const retryRaw = await this.redis.eval<
      typeof argv,
      string | CreditMutationResult<TMetadata>
    >(MUTATE_CREDITS_AND_ENQUEUE_LUA, [stateKey, this.queueKey], argv);
    return typeof retryRaw === 'string'
      ? (JSON.parse(retryRaw) as CreditMutationResult<TMetadata>)
      : retryRaw;
  }

  public async reserve(
    input: CreditMutationInput<TMetadata>,
  ): Promise<CreditMutationResult<TMetadata>> {
    return this.mutate('reserve', input);
  }

  public async settle(
    input: CreditMutationInput<TMetadata>,
  ): Promise<CreditMutationResult<TMetadata>> {
    return this.mutate('settle', input);
  }

  public async release(
    input: Omit<CreditMutationInput<TMetadata>, 'amount'> & { amount?: number },
  ): Promise<CreditMutationResult<TMetadata>> {
    return this.mutate('release', {
      ...input,
      amount: input.amount ?? 0,
    });
  }

  public async dequeue(
    count = 100,
  ): Promise<Array<CreditLedgerEvent<TMetadata>>> {
    const raw = await this.redis.lpop<
      Array<string | CreditLedgerEvent<TMetadata>>
    >(this.queueKey, Math.max(1, Math.trunc(count)));
    if (!raw) {
      return [];
    }
    const items = Array.isArray(raw) ? raw : [raw];
    return items.map((item) =>
      typeof item === 'string'
        ? (JSON.parse(item) as CreditLedgerEvent<TMetadata>)
        : item,
    );
  }
}
