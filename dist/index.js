// src/index.ts
var DEFAULT_PREFIX = "credit-queue";
var DEFAULT_STATE_TTL_SECONDS = 35 * 24 * 60 * 60;
var SEED_BALANCE_LUA = `
local stateKey = KEYS[1]
local key = ARGV[1]
local budget = tonumber(ARGV[2]) or 0
local consumed = tonumber(ARGV[3]) or 0
local version = tonumber(ARGV[4]) or 0
local ttlSeconds = tonumber(ARGV[5]) or 3024000

if redis.call('EXISTS', stateKey) == 0 then
  redis.call(
    'HSET',
    stateKey,
    'key', key,
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
    'key', key,
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
  key = key,
  budget = finalBudget,
  consumed = finalConsumed,
  reserved = finalReserved,
  available = finalBudget - (finalConsumed + finalReserved),
  version = finalVersion
})
`;
var MUTATE_CREDITS_AND_ENQUEUE_LUA = `
local stateKey = KEYS[1]
local queueKey = KEYS[2]

local operation = ARGV[1]
local amount = tonumber(ARGV[2])
local key = ARGV[3]
local requestId = ARGV[4]
local createdAt = ARGV[5]
local stateTtlSeconds = tonumber(ARGV[6]) or 3024000
local allowOverdraft = ARGV[7] == '1'
local metadataJson = ARGV[8]

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
    key = key,
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
  key = key,
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
var CreditQueue = class {
  redis;
  prefix;
  queueKey;
  stateTtlSeconds;
  loadBalance;
  constructor(config) {
    this.redis = config.redis;
    this.prefix = config.prefix ?? DEFAULT_PREFIX;
    this.queueKey = config.queueKey ?? `${this.prefix}:events:queue`;
    this.stateTtlSeconds = config.stateTtlSeconds ?? DEFAULT_STATE_TTL_SECONDS;
    this.loadBalance = config.loadBalance;
  }
  stateKey(key) {
    return `${this.prefix}:${key}:state`;
  }
  async seedBalance(params) {
    const raw = await this.redis.eval(
      SEED_BALANCE_LUA,
      [this.stateKey(params.key)],
      [
        params.key,
        Math.trunc(params.budget),
        Math.trunc(params.consumed ?? 0),
        Math.trunc(params.version ?? 0),
        params.ttlSeconds ?? this.stateTtlSeconds
      ]
    );
    return typeof raw === "string" ? JSON.parse(raw) : raw;
  }
  async getState(key) {
    const raw = await this.redis.hgetall(
      this.stateKey(key)
    );
    if (!raw || Object.keys(raw).length === 0) {
      return null;
    }
    const budget = Number(raw.budget ?? 0);
    const consumed = Number(raw.consumed ?? 0);
    const reserved = Number(raw.reserved ?? 0);
    const version = Number(raw.version ?? 0);
    return {
      key: String(raw.key ?? key),
      budget,
      consumed,
      reserved,
      available: budget - (consumed + reserved),
      version
    };
  }
  async mutate(operation, input) {
    if (!Number.isFinite(input.amount) || input.amount < 0) {
      throw new Error(`Invalid credit amount: ${input.amount}`);
    }
    const stateKey = this.stateKey(input.key);
    const argv = [
      operation,
      Math.trunc(input.amount),
      input.key,
      input.requestId,
      input.createdAt ?? (/* @__PURE__ */ new Date()).toISOString(),
      this.stateTtlSeconds,
      input.allowOverdraft ? "1" : "0",
      input.metadata ? JSON.stringify(input.metadata) : ""
    ];
    const firstRaw = await this.redis.eval(MUTATE_CREDITS_AND_ENQUEUE_LUA, [stateKey, this.queueKey], argv);
    const firstAttempt = typeof firstRaw === "string" ? JSON.parse(firstRaw) : firstRaw;
    if (firstAttempt.status !== "uninitialized" || !this.loadBalance) {
      return firstAttempt;
    }
    const loaded = await this.loadBalance(input.key);
    if (!loaded) {
      return firstAttempt;
    }
    await this.seedBalance(loaded);
    const retryRaw = await this.redis.eval(MUTATE_CREDITS_AND_ENQUEUE_LUA, [stateKey, this.queueKey], argv);
    return typeof retryRaw === "string" ? JSON.parse(retryRaw) : retryRaw;
  }
  async reserve(input) {
    return this.mutate("reserve", input);
  }
  async settle(input) {
    return this.mutate("settle", input);
  }
  async release(input) {
    return this.mutate("release", {
      ...input,
      amount: input.amount ?? 0
    });
  }
  async dequeue(count = 100) {
    const raw = await this.redis.lpop(this.queueKey, Math.max(1, Math.trunc(count)));
    if (!raw) {
      return [];
    }
    const items = Array.isArray(raw) ? raw : [raw];
    return items.map(
      (item) => typeof item === "string" ? JSON.parse(item) : item
    );
  }
};
export {
  CreditQueue,
  MUTATE_CREDITS_AND_ENQUEUE_LUA,
  SEED_BALANCE_LUA
};
