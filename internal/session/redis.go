package session

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	redisPrefix  = "tide:stream:"
	tenantPrefix = "tide:tenant:"
)

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(addr, password string, db int) *RedisStore {
	return &RedisStore{client: redis.NewClient(&redis.Options{
		Addr: addr, Password: password, DB: db,
		ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second,
	})}
}

func (s *RedisStore) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

func (s *RedisStore) Health(ctx context.Context) error { return s.Ping(ctx) }

var createScript = redis.NewScript(`
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[1])
if redis.call("ZCARD", KEYS[2]) >= tonumber(ARGV[2]) then return 0 end
if redis.call("EXISTS", KEYS[1]) == 1 then return -1 end
redis.call("HSET", KEYS[1],
  "id", ARGV[3], "tenant", ARGV[4], "language", ARGV[5],
  "state", ARGV[6], "generation", "0", "epoch", "0", "owner_id", "",
  "owner_addr", "", "owner_lease_ms", "0", "next_offset", "0",
  "token_hash", ARGV[7], "created_ms", ARGV[8], "expires_ms", ARGV[9],
  "detached_until_ms", "0")
redis.call("PEXPIREAT", KEYS[1], ARGV[9] + ARGV[10])
redis.call("ZADD", KEYS[2], ARGV[9], ARGV[3])
redis.call("PEXPIREAT", KEYS[2], ARGV[9] + ARGV[10])
return 1
`)

func (s *RedisStore) Create(ctx context.Context, stream Session, tenantLimit int) error {
	now := time.Now().UnixMilli()
	result, err := createScript.Run(ctx, s.client,
		[]string{redisPrefix + stream.ID, tenantPrefix + stream.TenantID},
		now, tenantLimit, stream.ID, stream.TenantID, stream.LanguageCode,
		string(stream.State), stream.TokenHash, stream.CreatedAt.UnixMilli(), stream.ExpiresAt.UnixMilli(),
		int64((5*time.Minute)/time.Millisecond)).Int()
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}
	if result == 0 {
		return ErrQuotaExceeded
	}
	if result == -1 {
		return fmt.Errorf("stream already exists")
	}
	return nil
}

func (s *RedisStore) Get(ctx context.Context, streamID string) (Session, error) {
	values, err := s.client.HGetAll(ctx, redisPrefix+streamID).Result()
	if err != nil {
		return Session{}, fmt.Errorf("get stream: %w", err)
	}
	if len(values) == 0 {
		return Session{}, ErrNotFound
	}
	stream, err := decodeSession(values)
	if err != nil {
		return Session{}, err
	}
	if time.Now().After(stream.ExpiresAt) {
		return Session{}, ErrExpired
	}
	return stream, nil
}

var attachScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then return {-1} end
local values = redis.call("HMGET", KEYS[1], "tenant", "state", "generation", "token_hash", "expires_ms")
if values[1] ~= ARGV[1] then return {-2} end
if values[2] == "ended" or values[2] == "failed" then return {-3} end
if tonumber(values[3]) ~= tonumber(ARGV[2]) or values[4] ~= ARGV[3] then return {-4} end
if tonumber(values[5]) <= tonumber(ARGV[5]) then return {-5} end
local generation = tonumber(values[3]) + 1
redis.call("HSET", KEYS[1], "generation", generation, "token_hash", ARGV[4],
  "state", "attached", "detached_until_ms", "0")
return {generation}
`)

func (s *RedisStore) Attach(ctx context.Context, streamID, tenantID string, expectedGeneration uint64, tokenHash, nextTokenHash string) (Session, error) {
	result, err := attachScript.Run(ctx, s.client, []string{redisPrefix + streamID},
		tenantID, expectedGeneration, tokenHash, nextTokenHash, time.Now().UnixMilli()).Int64Slice()
	if err != nil {
		return Session{}, fmt.Errorf("attach stream: %w", err)
	}
	if len(result) == 0 {
		return Session{}, fmt.Errorf("attach stream returned no result")
	}
	switch result[0] {
	case -1:
		return Session{}, ErrNotFound
	case -2:
		return Session{}, ErrForbidden
	case -3:
		return Session{}, ErrEnded
	case -4:
		return Session{}, ErrTokenConsumed
	case -5:
		return Session{}, ErrExpired
	}
	return s.Get(ctx, streamID)
}

var detachScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then return 0 end
local values = redis.call("HMGET", KEYS[1], "generation", "state")
if tonumber(values[1]) == tonumber(ARGV[1]) and values[2] ~= "ended" then
  redis.call("HSET", KEYS[1], "state", "detached", "detached_until_ms", ARGV[2])
end
return 1
`)

func (s *RedisStore) MarkDetached(ctx context.Context, streamID string, generation uint64, until time.Time) error {
	return detachScript.Run(ctx, s.client, []string{redisPrefix + streamID},
		generation, until.UnixMilli()).Err()
}

var offsetScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then return 0 end
local values = redis.call("HMGET", KEYS[1], "generation", "next_offset")
if tonumber(values[1]) == tonumber(ARGV[1]) and tonumber(ARGV[2]) > tonumber(values[2]) then
  redis.call("HSET", KEYS[1], "next_offset", ARGV[2])
end
return 1
`)

func (s *RedisStore) UpdateOffset(ctx context.Context, streamID string, generation, nextOffset uint64) error {
	return offsetScript.Run(ctx, s.client, []string{redisPrefix + streamID},
		generation, nextOffset).Err()
}

var ownerScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then return {-1, 0, 0} end
local values = redis.call("HMGET", KEYS[1], "state", "owner_id", "owner_lease_ms", "epoch", "expires_ms")
if values[1] == "ended" or values[1] == "failed" then return {-2, 0, 0} end
if tonumber(values[5]) <= tonumber(ARGV[3]) then return {-3, 0, 0} end
local previous = values[2]
if previous ~= "" and previous ~= ARGV[1] and tonumber(values[3]) > tonumber(ARGV[3]) then
  return {0, tonumber(values[4]), 0}
end
local epoch = tonumber(values[4])
local changed = 0
if previous == "" then
  epoch = 1
elseif previous ~= ARGV[1] then
  epoch = epoch + 1
  changed = 1
end
redis.call("HSET", KEYS[1], "owner_id", ARGV[1], "owner_addr", ARGV[2],
  "owner_lease_ms", ARGV[4], "epoch", epoch)
return {1, epoch, changed}
`)

func (s *RedisStore) AcquireOwner(ctx context.Context, streamID, nodeID, nodeAddr string, now time.Time, lease time.Duration) (Session, bool, bool, error) {
	result, err := ownerScript.Run(ctx, s.client, []string{redisPrefix + streamID},
		nodeID, nodeAddr, now.UnixMilli(), now.Add(lease).UnixMilli()).Int64Slice()
	if err != nil {
		return Session{}, false, false, fmt.Errorf("acquire owner: %w", err)
	}
	if len(result) != 3 {
		return Session{}, false, false, fmt.Errorf("acquire owner returned invalid result")
	}
	if result[0] == -1 {
		return Session{}, false, false, ErrNotFound
	}
	if result[0] == -2 {
		return Session{}, false, false, ErrEnded
	}
	if result[0] == -3 {
		return Session{}, false, false, ErrExpired
	}
	stream, err := s.Get(ctx, streamID)
	return stream, result[0] == 1, result[2] == 1, err
}

var renewScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then return -1 end
if redis.call("HGET", KEYS[1], "owner_id") ~= ARGV[1] then return 0 end
local values = redis.call("HMGET", KEYS[1], "state", "expires_ms")
local state = values[1]
if state == "ended" or state == "failed" then return -2 end
if tonumber(values[2]) <= tonumber(ARGV[3]) then return -3 end
redis.call("HSET", KEYS[1], "owner_lease_ms", ARGV[2])
return 1
`)

func (s *RedisStore) RenewOwner(ctx context.Context, streamID, nodeID string, now time.Time, lease time.Duration) error {
	result, err := renewScript.Run(ctx, s.client, []string{redisPrefix + streamID},
		nodeID, now.Add(lease).UnixMilli(), now.UnixMilli()).Int()
	if err != nil {
		return fmt.Errorf("renew owner: %w", err)
	}
	if result == -1 {
		return ErrNotFound
	}
	if result == 0 {
		return ErrOwnerConflict
	}
	if result == -2 {
		return ErrEnded
	}
	if result == -3 {
		return ErrExpired
	}
	return nil
}

var releaseScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then return -1 end
if redis.call("HGET", KEYS[1], "owner_id") ~= ARGV[1] then return 0 end
redis.call("HSET", KEYS[1], "owner_lease_ms", "0")
return 1
`)

func (s *RedisStore) ReleaseOwner(ctx context.Context, streamID, nodeID string) error {
	result, err := releaseScript.Run(ctx, s.client, []string{redisPrefix + streamID}, nodeID).Int()
	if err != nil {
		return fmt.Errorf("release owner: %w", err)
	}
	if result == -1 {
		return ErrNotFound
	}
	if result == 0 {
		return ErrOwnerConflict
	}
	return nil
}

var endScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then return 0 end
local tenant = redis.call("HGET", KEYS[1], "tenant")
if ARGV[1] ~= "" and tenant ~= ARGV[1] then return -1 end
redis.call("HSET", KEYS[1], "state", "ended", "detached_until_ms", "0")
redis.call("PEXPIRE", KEYS[1], ARGV[2])
redis.call("ZREM", "tide:tenant:" .. tenant, ARGV[3])
return 1
`)

func (s *RedisStore) End(ctx context.Context, streamID, tenantID, _ string, retention time.Duration) error {
	result, err := endScript.Run(ctx, s.client, []string{redisPrefix + streamID},
		tenantID, retention.Milliseconds(), streamID).Int()
	if err != nil {
		return fmt.Errorf("end stream: %w", err)
	}
	if result == -1 {
		return ErrForbidden
	}
	return nil
}

func (s *RedisStore) Close() error { return s.client.Close() }

func decodeSession(values map[string]string) (Session, error) {
	uintValue := func(key string) (uint64, error) {
		return strconv.ParseUint(values[key], 10, 64)
	}
	intValue := func(key string) (int64, error) {
		if values[key] == "" {
			return 0, nil
		}
		return strconv.ParseInt(values[key], 10, 64)
	}
	timeValue := func(milliseconds int64) time.Time {
		if milliseconds == 0 {
			return time.Time{}
		}
		return time.UnixMilli(milliseconds)
	}
	generation, err := uintValue("generation")
	if err != nil {
		return Session{}, fmt.Errorf("decode generation: %w", err)
	}
	epoch, err := uintValue("epoch")
	if err != nil {
		return Session{}, fmt.Errorf("decode epoch: %w", err)
	}
	nextOffset, err := uintValue("next_offset")
	if err != nil {
		return Session{}, fmt.Errorf("decode offset: %w", err)
	}
	created, _ := intValue("created_ms")
	expires, _ := intValue("expires_ms")
	lease, _ := intValue("owner_lease_ms")
	detached, _ := intValue("detached_until_ms")
	return Session{
		ID: values["id"], TenantID: values["tenant"],
		LanguageCode: values["language"], State: State(values["state"]),
		Generation: generation, Epoch: epoch, OwnerID: values["owner_id"],
		OwnerAddr: values["owner_addr"], OwnerLeaseEnd: timeValue(lease),
		NextOffset: nextOffset, TokenHash: values["token_hash"],
		CreatedAt: timeValue(created), ExpiresAt: timeValue(expires),
		DetachedUntil: timeValue(detached),
	}, nil
}
