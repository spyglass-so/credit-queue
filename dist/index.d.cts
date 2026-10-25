type CreditOperation = 'reserve' | 'settle' | 'release';
type CreditQueueEvent<TMetadata extends Record<string, unknown> = Record<string, unknown>> = {
    key: string;
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
type CreditBalanceState = {
    key: string;
    budget: number;
    consumed: number;
    reserved: number;
    available: number;
    version: number;
};
type CreditMutationStatus = 'applied' | 'insufficient_credits' | 'uninitialized' | 'no_active_reservation' | 'invalid_state';
type CreditMutationResult<TMetadata extends Record<string, unknown> = Record<string, unknown>> = {
    ok: boolean;
    status: CreditMutationStatus;
    reason?: string;
    state: CreditBalanceState | null;
    event: CreditQueueEvent<TMetadata> | null;
};
type CreditMutationInput<TMetadata extends Record<string, unknown> = Record<string, unknown>> = {
    key: string;
    requestId: string;
    amount: number;
    allowOverdraft?: boolean;
    createdAt?: string;
    metadata?: TMetadata;
};
type SeedBalanceInput = {
    key: string;
    budget: number;
    consumed?: number;
    version?: number;
    ttlSeconds?: number;
};
interface CreditQueueRedisClient {
    eval<TArgs extends unknown[], TData = unknown>(script: string, keys: string[], args: TArgs): Promise<TData>;
    hgetall<TData extends Record<string, unknown> = Record<string, unknown>>(key: string): Promise<TData | null>;
    lpop<TData = unknown>(key: string, count?: number): Promise<TData | null>;
}
type CreditQueueConfig<TMetadata extends Record<string, unknown> = Record<string, unknown>> = {
    redis: CreditQueueRedisClient;
    prefix?: string;
    queueKey?: string;
    stateTtlSeconds?: number;
    loadBalance?: (key: string) => Promise<SeedBalanceInput | null>;
    __metadataType?: TMetadata;
};
declare const SEED_BALANCE_LUA = "\nlocal stateKey = KEYS[1]\nlocal key = ARGV[1]\nlocal budget = tonumber(ARGV[2]) or 0\nlocal consumed = tonumber(ARGV[3]) or 0\nlocal version = tonumber(ARGV[4]) or 0\nlocal ttlSeconds = tonumber(ARGV[5]) or 3024000\n\nif redis.call('EXISTS', stateKey) == 0 then\n  redis.call(\n    'HSET',\n    stateKey,\n    'key', key,\n    'budget', budget,\n    'consumed', consumed,\n    'reserved', 0,\n    'version', version\n  )\n  redis.call('EXPIRE', stateKey, ttlSeconds)\nelse\n  local currentVersion = tonumber(redis.call('HGET', stateKey, 'version') or '0')\n  redis.call(\n    'HSET',\n    stateKey,\n    'key', key,\n    'budget', budget\n  )\n  if version > currentVersion then\n    redis.call(\n      'HSET',\n      stateKey,\n      'consumed', consumed,\n      'version', version\n    )\n  end\n  if redis.call('TTL', stateKey) == -1 then\n    redis.call('EXPIRE', stateKey, ttlSeconds)\n  end\nend\n\nlocal finalBudget = tonumber(redis.call('HGET', stateKey, 'budget') or '0')\nlocal finalConsumed = tonumber(redis.call('HGET', stateKey, 'consumed') or '0')\nlocal finalReserved = tonumber(redis.call('HGET', stateKey, 'reserved') or '0')\nlocal finalVersion = tonumber(redis.call('HGET', stateKey, 'version') or '0')\n\nreturn cjson.encode({\n  key = key,\n  budget = finalBudget,\n  consumed = finalConsumed,\n  reserved = finalReserved,\n  available = finalBudget - (finalConsumed + finalReserved),\n  version = finalVersion\n})\n";
declare const MUTATE_CREDITS_AND_ENQUEUE_LUA = "\nlocal stateKey = KEYS[1]\nlocal queueKey = KEYS[2]\n\nlocal operation = ARGV[1]\nlocal amount = tonumber(ARGV[2])\nlocal key = ARGV[3]\nlocal requestId = ARGV[4]\nlocal createdAt = ARGV[5]\nlocal stateTtlSeconds = tonumber(ARGV[6]) or 3024000\nlocal allowOverdraft = ARGV[7] == '1'\nlocal metadataJson = ARGV[8]\n\nif not amount or amount < 0 or math.floor(amount) ~= amount then\n  return cjson.encode({\n    ok = false,\n    status = 'invalid_state',\n    reason = 'invalid_amount',\n    state = cjson.null,\n    event = cjson.null\n  })\nend\n\nif redis.call('EXISTS', stateKey) == 0 then\n  return cjson.encode({\n    ok = false,\n    status = 'uninitialized',\n    state = cjson.null,\n    event = cjson.null\n  })\nend\n\nlocal budget = tonumber(redis.call('HGET', stateKey, 'budget') or '0')\nlocal consumed = tonumber(redis.call('HGET', stateKey, 'consumed') or '0')\nlocal reserved = tonumber(redis.call('HGET', stateKey, 'reserved') or '0')\nlocal version = tonumber(redis.call('HGET', stateKey, 'version') or '0')\n\nlocal function buildState()\n  return {\n    key = key,\n    budget = budget,\n    consumed = consumed,\n    reserved = reserved,\n    available = budget - (consumed + reserved),\n    version = version\n  }\nend\n\nlocal resField = 'res:' .. requestId\nlocal reservedAmount = 0\nlocal eventAmount = amount\n\nif operation == 'reserve' then\n  if redis.call('HEXISTS', stateKey, resField) == 1 then\n    return cjson.encode({\n      ok = false,\n      status = 'invalid_state',\n      reason = 'reservation_already_exists',\n      state = buildState(),\n      event = cjson.null\n    })\n  end\n\n  local available = budget - (consumed + reserved)\n  if not allowOverdraft and available < amount then\n    return cjson.encode({\n      ok = false,\n      status = 'insufficient_credits',\n      state = buildState(),\n      event = cjson.null\n    })\n  end\n\n  reserved = reserved + amount\n  reservedAmount = amount\n  version = version + 1\n\n  redis.call(\n    'HSET',\n    stateKey,\n    'reserved', reserved,\n    'version', version,\n    resField, amount\n  )\nelseif operation == 'settle' then\n  if redis.call('HEXISTS', stateKey, resField) == 1 then\n    reservedAmount = tonumber(redis.call('HGET', stateKey, resField) or '0')\n    reserved = math.max(0, reserved - reservedAmount)\n    redis.call('HDEL', stateKey, resField)\n  end\n\n  consumed = consumed + amount\n  version = version + 1\n\n  redis.call(\n    'HSET',\n    stateKey,\n    'consumed', consumed,\n    'reserved', reserved,\n    'version', version\n  )\nelseif operation == 'release' then\n  if redis.call('HEXISTS', stateKey, resField) == 0 then\n    return cjson.encode({\n      ok = true,\n      status = 'no_active_reservation',\n      state = buildState(),\n      event = cjson.null\n    })\n  end\n\n  reservedAmount = tonumber(redis.call('HGET', stateKey, resField) or '0')\n  local releaseAmount = reservedAmount\n  if amount > 0 and amount < reservedAmount then\n    releaseAmount = amount\n  end\n  eventAmount = releaseAmount\n  reserved = math.max(0, reserved - releaseAmount)\n  version = version + 1\n\n  redis.call('HDEL', stateKey, resField)\n  redis.call(\n    'HSET',\n    stateKey,\n    'reserved', reserved,\n    'version', version\n  )\nelse\n  return cjson.encode({\n    ok = false,\n    status = 'invalid_state',\n    reason = 'unknown_operation',\n    state = buildState(),\n    event = cjson.null\n  })\nend\n\nif redis.call('TTL', stateKey) == -1 then\n  redis.call('EXPIRE', stateKey, stateTtlSeconds)\nend\n\nlocal nextState = buildState()\nlocal event = {\n  key = key,\n  requestId = requestId,\n  operation = operation,\n  amount = eventAmount,\n  reservedAmount = reservedAmount,\n  budget = budget,\n  consumed = consumed,\n  reserved = reserved,\n  available = nextState.available,\n  version = version,\n  createdAt = createdAt\n}\n\nif metadataJson and metadataJson ~= '' then\n  event.metadata = cjson.decode(metadataJson)\nend\n\nredis.call('RPUSH', queueKey, cjson.encode(event))\n\nreturn cjson.encode({\n  ok = true,\n  status = 'applied',\n  state = nextState,\n  event = event\n})\n";
declare class CreditQueue<TMetadata extends Record<string, unknown> = Record<string, unknown>> {
    private readonly redis;
    private readonly prefix;
    readonly queueKey: string;
    private readonly stateTtlSeconds;
    private readonly loadBalance?;
    constructor(config: CreditQueueConfig<TMetadata>);
    stateKey(key: string): string;
    seedBalance(params: SeedBalanceInput): Promise<CreditBalanceState>;
    getState(key: string): Promise<CreditBalanceState | null>;
    mutate(operation: CreditOperation, input: CreditMutationInput<TMetadata>): Promise<CreditMutationResult<TMetadata>>;
    reserve(input: CreditMutationInput<TMetadata>): Promise<CreditMutationResult<TMetadata>>;
    settle(input: CreditMutationInput<TMetadata>): Promise<CreditMutationResult<TMetadata>>;
    release(input: Omit<CreditMutationInput<TMetadata>, 'amount'> & {
        amount?: number;
    }): Promise<CreditMutationResult<TMetadata>>;
    dequeue(count?: number): Promise<Array<CreditQueueEvent<TMetadata>>>;
}

export { type CreditBalanceState, type CreditMutationInput, type CreditMutationResult, type CreditMutationStatus, type CreditOperation, CreditQueue, type CreditQueueConfig, type CreditQueueEvent, type CreditQueueRedisClient, MUTATE_CREDITS_AND_ENQUEUE_LUA, SEED_BALANCE_LUA, type SeedBalanceInput };
