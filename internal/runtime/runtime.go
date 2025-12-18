package runtime

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/UniQw/uniqw-go/internal/hctx"
	ikeys "github.com/UniQw/uniqw-go/internal/keys"
	"github.com/UniQw/uniqw-go/internal/worker"
	"github.com/redis/go-redis/v9"
)

// ErrNoHandler indicates there is no handler for the task type; the runtime will move the task to dead without retry.
var ErrNoHandler = errors.New("no handler")

// Logger is a minimal logging interface used internally by the runtime.
// It mirrors the public logger in the root package to avoid an import cycle.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Debugf(string, ...any) {}
func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Warnf(string, ...any)  {}
func (noopLogger) Errorf(string, ...any) {}

type Config struct {
	Queues        map[string]int
	Concurrency   int
	VisibilityTTL time.Duration
	Logger        Logger
}

// Executor executes a task payload for a given type.
type Executor func(ctx context.Context, taskType string, payload []byte) error

type Runtime struct {
	rdb       redis.UniversalClient
	cfg       Config
	exec      Executor
	wg        sync.WaitGroup
	mu        sync.Mutex
	started   bool
	ctx       context.Context
	cancel    context.CancelFunc
	queueList []string
	qmap      map[string]ikeys.Queue
	log       Logger
}

// scheduleOneScript atomically moves one due item from delayed ZSET to pending LIST.
// It returns the moved member on success, or false/nil if none moved.
var scheduleOneScript = redis.NewScript(`
local dkey = KEYS[1]
local pkey = KEYS[2]
local now  = ARGV[1]
local items = redis.call('ZRANGEBYSCORE', dkey, '-inf', now, 'LIMIT', 0, 1)
if #items == 0 then return false end
local m = items[1]
local rem = redis.call('ZREM', dkey, m)
if rem == 1 then
  redis.call('LPUSH', pkey, m)
  return m
end
return false
`)

// reclaimOneScript atomically reclaims one expired active item back to pending.
var reclaimOneScript = redis.NewScript(`
local akey = KEYS[1]
local pkey = KEYS[2]
local now  = ARGV[1]
local items = redis.call('ZRANGEBYSCORE', akey, '-inf', now, 'LIMIT', 0, 1)
if #items == 0 then return false end
local m = items[1]
local rem = redis.call('ZREM', akey, m)
if rem == 1 then
  redis.call('LPUSH', pkey, m)
  return m
end
return false
`)

// expireOneScript atomically fails one expired job if it hasn't started yet.
// It tries to remove from delayed or pending, then pushes to dead and removes from expiry.
var expireOneScript = redis.NewScript(`
local xkey = KEYS[1] -- expiry
local dkey = KEYS[2] -- delayed
local pkey = KEYS[3] -- pending
local dekey = KEYS[4] -- dead
local now  = ARGV[1]
local items = redis.call('ZRANGEBYSCORE', xkey, '-inf', now, 'LIMIT', 0, 1)
if #items == 0 then return false end
local m = items[1]
-- try from delayed first
local remd = redis.call('ZREM', dkey, m)
if remd == 1 then
  redis.call('LPUSH', dekey, m)
  redis.call('ZREM', xkey, m)
  return m
end
-- try from pending list
local remp = redis.call('LREM', pkey, 1, m)
if remp > 0 then
  redis.call('LPUSH', dekey, m)
  redis.call('ZREM', xkey, m)
  return m
end
-- not in delayed/pending; remove from expiry to avoid spinning (likely already active or processed)
redis.call('ZREM', xkey, m)
return false
`)

// New creates a new background runtime that manages workers and maintenance routines.
func New(rdb redis.UniversalClient, cfg Config, exec Executor) *Runtime {
	ctx, cancel := context.WithCancel(context.Background())
	qmap := make(map[string]ikeys.Queue, len(cfg.Queues))
	for q := range cfg.Queues {
		qmap[q] = ikeys.For(q)
	}
	lg := cfg.Logger
	if lg == nil {
		lg = noopLogger{}
	}
	return &Runtime{
		rdb:       rdb,
		cfg:       cfg,
		exec:      exec,
		ctx:       ctx,
		cancel:    cancel,
		queueList: expandQueues(cfg.Queues),
		qmap:      qmap,
		log:       lg,
	}
}

// Start launches workers and background maintenance goroutines.
func (rt *Runtime) Start() {
	rt.mu.Lock()
	if rt.started {
		rt.log.Warnf("runtime already started; ignoring Start()")
		rt.mu.Unlock()
		return
	}
	rt.started = true
	rt.mu.Unlock()
	rt.log.Infof("runtime starting: concurrency=%d queues=%d", rt.cfg.Concurrency, len(rt.cfg.Queues))

	// One-time key migration: move old non-hashtag keys to new cluster-safe hashtagged keys.
	for q := range rt.cfg.Queues {
		rt.migrateQueueKeys(q)
	}

	// One-time migration: ensure succeeded key uses ZSET (not LIST) to avoid WRONGTYPE.
	// If we detect a legacy LIST, we move its items into a ZSET with a far-future score.
	for q := range rt.cfg.Queues {
		k := rt.qmap[q].Succeeded
		typ, err := rt.rdb.Type(rt.ctx, k).Result()
		if err != nil {
			rt.log.Warnf("migration: TYPE failed queue=%s err=%v", q, err)
			continue
		}
		if typ == "list" {
			items, lerr := rt.rdb.LRange(rt.ctx, k, 0, -1).Result()
			if lerr != nil {
				rt.log.Warnf("migration: LRange failed queue=%s err=%v", q, lerr)
				continue
			}
			if derr := rt.rdb.Del(rt.ctx, k).Err(); derr != nil {
				rt.log.Warnf("migration: DEL failed queue=%s err=%v", q, derr)
				continue
			}
			if len(items) > 0 {
				zs := make([]redis.Z, 0, len(items))
				// Far future score for non-expiring entries
				const farFuture = 1 << 62
				for _, it := range items {
					zs = append(zs, redis.Z{Score: float64(farFuture), Member: it})
				}
				if zerr := rt.rdb.ZAdd(rt.ctx, k, zs...).Err(); zerr != nil {
					rt.log.Warnf("migration: ZAdd failed queue=%s err=%v", q, zerr)
				} else {
					rt.log.Infof("migration: succeeded LIST->ZSET migrated=%d queue=%s", len(items), q)
				}
			} else {
				// create empty ZSET to lock type
				if zerr := rt.rdb.ZAdd(rt.ctx, k, redis.Z{Score: 0, Member: "_init_"}).Err(); zerr == nil {
					_ = rt.rdb.ZRem(rt.ctx, k, "_init_").Err()
					rt.log.Infof("migration: succeeded LIST->ZSET migrated=0 queue=%s", q)
				}
			}
		} else if typ != "zset" && typ != "none" {
			rt.log.Warnf("migration: unexpected type for succeeded key queue=%s type=%s", q, typ)
		}
	}

	// workers
	for i := 0; i < rt.cfg.Concurrency; i++ {
		rt.wg.Add(1)
		seed := time.Now().UnixNano() + int64(i)
		rng := rand.New(rand.NewSource(seed))
		go func(r *rand.Rand) {
			defer rt.wg.Done()
			rt.workerLoop(r)
		}(rng)
	}

	// Per-queue maintenance goroutines
	for q := range rt.cfg.Queues {
		// Retention cleaner (for succeeded ZSET)
		rt.wg.Add(1)
		go func(queue string) {
			defer rt.wg.Done()
			// Cleaner runs every 1s for smoother expirations and lower burst deletes
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			key := rt.qmap[queue].Succeeded
			for {
				select {
				case <-rt.ctx.Done():
					return
				case <-ticker.C:
					// Backward-compat: remove legacy second-precision scores
					nowSec := time.Now().Unix()
					if err := rt.rdb.ZRemRangeByScore(rt.ctx, key, "0", strconv.FormatInt(nowSec, 10)).Err(); err != nil {
						rt.log.Warnf("cleaner: sec sweep failed queue=%s err=%v", queue, err)
					}
					// New: remove millisecond-precision scores
					nowMs := time.Now().UnixMilli()
					if err := rt.rdb.ZRemRangeByScore(rt.ctx, key, "0", strconv.FormatInt(nowMs, 10)).Err(); err != nil {
						rt.log.Warnf("cleaner: ms sweep failed queue=%s err=%v", queue, err)
					}
				}
			}
		}(q)

		// Dead cleaner: purge expired entries from dead list based on index ZSET
		rt.wg.Add(1)
		go func(queue string) {
			defer rt.wg.Done()
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			kset := rt.qmap[queue]
			for {
				select {
				case <-rt.ctx.Done():
					return
				case <-ticker.C:
					nowMs := strconv.FormatInt(time.Now().UnixMilli(), 10)
					// fetch a small batch to avoid long blocking operations
					members, err := rt.rdb.ZRangeByScore(rt.ctx, kset.DeadExpiry, &redis.ZRangeBy{Min: "0", Max: nowMs, Offset: 0, Count: 256}).Result()
					if err != nil && err != redis.Nil {
						rt.log.Warnf("dead-cleaner: range failed queue=%s err=%v", queue, err)
						continue
					}
					if len(members) == 0 {
						continue
					}
					// remove each member from list and index atomically in a pipeline
					_, pipErr := rt.rdb.TxPipelined(rt.ctx, func(p redis.Pipeliner) error {
						for _, m := range members {
							p.LRem(rt.ctx, kset.Dead, 1, m)
							p.ZRem(rt.ctx, kset.DeadExpiry, m)
						}
						return nil
					})
					if pipErr != nil {
						rt.log.Warnf("dead-cleaner: purge failed queue=%s err=%v", queue, pipErr)
					}
				}
			}
		}(q)

		// Delayed scheduler: move due tasks from delayed → pending atomically
		rt.wg.Add(1)
		go func(queue string) {
			defer rt.wg.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			kset := rt.qmap[queue]
			dkey := kset.Delayed
			pkey := kset.Pending
			for {
				select {
				case <-rt.ctx.Done():
					return
				case <-ticker.C:
					now := strconv.FormatInt(time.Now().Unix(), 10)
					// drain up to N per tick to avoid long loops
					for i := 0; i < 256; i++ {
						res, err := scheduleOneScript.Run(rt.ctx, rt.rdb, []string{dkey, pkey}, now).Result()
						if err == redis.Nil || res == nil || res == false {
							break
						}
						if err != nil {
							rt.log.Warnf("scheduler: script failed queue=%s err=%v", queue, err)
							break
						}
					}
				}
			}
		}(q)

		// Visibility reclaimer: move expired active back to pending atomically (retry is not incremented here)
		rt.wg.Add(1)
		go func(queue string) {
			defer rt.wg.Done()
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()
			kset := rt.qmap[queue]
			akey := kset.Active
			pkey := kset.Pending
			for {
				select {
				case <-rt.ctx.Done():
					return
				case <-ticker.C:
					now := strconv.FormatInt(time.Now().Unix(), 10)
					for i := 0; i < 256; i++ {
						res, err := reclaimOneScript.Run(rt.ctx, rt.rdb, []string{akey, pkey}, now).Result()
						if err == redis.Nil || res == nil || res == false {
							break
						}
						if err != nil {
							rt.log.Warnf("reclaimer: script failed queue=%s err=%v", queue, err)
							break
						}
					}
				}
			}
		}(q)

		// Expirer: move expired (not yet started) tasks to dead atomically
		rt.wg.Add(1)
		go func(queue string) {
			defer rt.wg.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			kset := rt.qmap[queue]
			xkey := kset.Expiry
			dkey := kset.Delayed
			pkey := kset.Pending
			dekey := kset.Dead
			for {
				select {
				case <-rt.ctx.Done():
					return
				case <-ticker.C:
					now := strconv.FormatInt(time.Now().UnixMilli(), 10)
					for i := 0; i < 256; i++ {
						res, err := expireOneScript.Run(rt.ctx, rt.rdb, []string{xkey, dkey, pkey, dekey}, now).Result()
						if err == redis.Nil || res == nil || res == false {
							break
						}
						if err != nil {
							rt.log.Warnf("expirer: script failed queue=%s err=%v", queue, err)
							break
						}
					}
				}
			}
		}(q)
	}
}

// Stop cancels the internal context and waits for all goroutines to exit.
func (rt *Runtime) Stop() {
	rt.mu.Lock()
	if !rt.started {
		rt.log.Warnf("runtime not started; ignoring Stop()")
		rt.mu.Unlock()
		return
	}
	rt.started = false
	rt.mu.Unlock()
	rt.log.Infof("runtime stopping")

	rt.cancel()
	rt.wg.Wait()
}

func (rt *Runtime) workerLoop(rng *rand.Rand) {
	ql := rt.queueList
	if len(ql) == 0 {
		return
	}
	for {
		select {
		case <-rt.ctx.Done():
			return
		default:
		}

		queue := ql[rng.Intn(len(ql))]
		kset := rt.qmap[queue]
		taskObj, raw := worker.DequeueTask(rt.ctx, rt.rdb, kset, rt.cfg.VisibilityTTL)
		if taskObj == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// Expiry guard: if deadline has passed, dead-letter without executing
		if taskObj.DeadlineMs > 0 && time.Now().UnixMilli() > taskObj.DeadlineMs {
			if e := worker.FailToDead(rt.ctx, rt.rdb, kset, taskObj, raw, "expired"); e != nil {
				rt.log.Errorf("expire->dead failed: id=%s type=%s queue=%s err=%v", taskObj.ID, taskObj.Type, queue, e)
			} else {
				rt.log.Warnf("expired: id=%s type=%s queue=%s", taskObj.ID, taskObj.Type, queue)
			}
			worker.Recycle(taskObj)
			continue
		}

		taskObj.StartedAt = time.Now().UnixMilli()
		// Attach handler state to context to capture progress/result
		st := hctx.New()
		hctxCtx := hctx.WithState(rt.ctx, st)
		if err := rt.exec(hctxCtx, taskObj.Type, taskObj.Payload); err != nil {
			if errors.Is(err, ErrNoHandler) {
				if e := worker.FailToDead(rt.ctx, rt.rdb, kset, taskObj, raw, "no handler"); e != nil {
					rt.log.Errorf("deadletter failed: id=%s type=%s queue=%s err=%v", taskObj.ID, taskObj.Type, queue, e)
				}
				rt.log.Warnf("no handler for task: id=%s type=%s queue=%s", taskObj.ID, taskObj.Type, queue)
			} else {
				// propagate handler-provided metadata before transition
				taskObj.Progress = st.Progress
				taskObj.Result = st.Result
				if e := worker.RetryOrDead(rt.ctx, rt.rdb, kset, taskObj, raw, err.Error()); e != nil {
					rt.log.Errorf("retry/dead transition failed: id=%s type=%s queue=%s err=%v", taskObj.ID, taskObj.Type, queue, e)
				} else {
					rt.log.Warnf("handler error: id=%s type=%s queue=%s err=%v", taskObj.ID, taskObj.Type, queue, err)
				}
			}
			worker.Recycle(taskObj)
			continue
		}

		if e := worker.Ack(rt.ctx, rt.rdb, kset, raw); e != nil {
			rt.log.Errorf("ack failed: id=%s type=%s queue=%s err=%v", taskObj.ID, taskObj.Type, queue, e)
		}
		// capture handler metadata on success too
		taskObj.Progress = st.Progress
		taskObj.Result = st.Result
		if e := worker.TrackSucceededWithTTL(rt.ctx, rt.rdb, kset, taskObj); e != nil {
			rt.log.Warnf("track succeeded failed: id=%s type=%s queue=%s err=%v", taskObj.ID, taskObj.Type, queue, e)
		} else {
			rt.log.Debugf("processed: id=%s type=%s queue=%s", taskObj.ID, taskObj.Type, queue)
		}
		// Release de-dup lock on success so IDs do not accumulate forever.
		if err := rt.rdb.SRem(rt.ctx, kset.Unique, taskObj.ID).Err(); err != nil {
			rt.log.Warnf("unique unlock failed: id=%s queue=%s err=%v", taskObj.ID, queue, err)
		}
		worker.Recycle(taskObj)
	}
}

// CfgConcurrency exposes configured worker concurrency.
func (rt *Runtime) CfgConcurrency() int { return rt.cfg.Concurrency }

// CfgQueues exposes configured queues mapping.
func (rt *Runtime) CfgQueues() map[string]int { return rt.cfg.Queues }

func expandQueues(q map[string]int) []string {
	// Rough preallocation
	n := 0
	for _, w := range q {
		n += w
	}
	out := make([]string, 0, n)
	for name, weight := range q {
		for i := 0; i < weight; i++ {
			out = append(out, name)
		}
	}
	return out
}

// migrateQueueKeys migrates keys from legacy format without hash tag
// (uniqw:<queue>:...) to the new cluster-safe format with hash tag
// (uniqw:{<queue>}:...). It copies data by type and removes old keys.
func (rt *Runtime) migrateQueueKeys(queue string) {
	// Helper to build old (legacy) keys without hash tag
	oldPrefix := "uniqw:" + queue + ":"
	old := struct {
		Pending, Active, Delayed, Dead, Succeeded, Unique, Expiry string
	}{
		Pending:   oldPrefix + "pending",
		Active:    oldPrefix + "active",
		Delayed:   oldPrefix + "delayed",
		Dead:      oldPrefix + "dead",
		Succeeded: oldPrefix + "succeeded",
		Unique:    oldPrefix + "unique",
		Expiry:    oldPrefix + "expiry",
	}
	newk := rt.qmap[queue]

	// List keys: pending, dead
	rt.migrateList(old.Pending, newk.Pending, queue)
	rt.migrateList(old.Dead, newk.Dead, queue)
	// ZSET keys: active, delayed, succeeded
	rt.migrateZSet(old.Active, newk.Active, queue)
	rt.migrateZSet(old.Delayed, newk.Delayed, queue)
	rt.migrateZSet(old.Succeeded, newk.Succeeded, queue)
	// SET key: unique
	rt.migrateSet(old.Unique, newk.Unique, queue)
	// ZSET key: expiry (may not exist yet)
	rt.migrateZSet(old.Expiry, newk.Expiry, queue)
}

func (rt *Runtime) migrateList(oldKey, newKey, queue string) {
	typOld, err := rt.rdb.Type(rt.ctx, oldKey).Result()
	if err != nil || typOld != "list" {
		return
	}
	typNew, _ := rt.rdb.Type(rt.ctx, newKey).Result()
	if typNew != "none" {
		return
	}
	vals, err := rt.rdb.LRange(rt.ctx, oldKey, 0, -1).Result()
	if err != nil || len(vals) == 0 {
		_ = rt.rdb.Del(rt.ctx, oldKey).Err()
		return
	}
	if err := rt.rdb.RPush(rt.ctx, newKey, vals).Err(); err != nil {
		rt.log.Warnf("migration: list copy failed queue=%s key=%s->%s err=%v", queue, oldKey, newKey, err)
		return
	}
	if err := rt.rdb.Del(rt.ctx, oldKey).Err(); err == nil {
		rt.log.Infof("migration: list moved queue=%s key=%s->%s count=%d", queue, oldKey, newKey, len(vals))
	}
}

func (rt *Runtime) migrateZSet(oldKey, newKey, queue string) {
	typOld, err := rt.rdb.Type(rt.ctx, oldKey).Result()
	if err != nil || typOld != "zset" {
		return
	}
	typNew, _ := rt.rdb.Type(rt.ctx, newKey).Result()
	if typNew != "none" {
		return
	}
	zs, err := rt.rdb.ZRangeWithScores(rt.ctx, oldKey, 0, -1).Result()
	if err != nil || len(zs) == 0 {
		_ = rt.rdb.Del(rt.ctx, oldKey).Err()
		return
	}
	if err := rt.rdb.ZAdd(rt.ctx, newKey, zs...).Err(); err != nil {
		rt.log.Warnf("migration: zset copy failed queue=%s key=%s->%s err=%v", queue, oldKey, newKey, err)
		return
	}
	if err := rt.rdb.Del(rt.ctx, oldKey).Err(); err == nil {
		rt.log.Infof("migration: zset moved queue=%s key=%s->%s count=%d", queue, oldKey, newKey, len(zs))
	}
}

func (rt *Runtime) migrateSet(oldKey, newKey, queue string) {
	typOld, err := rt.rdb.Type(rt.ctx, oldKey).Result()
	if err != nil || typOld != "set" {
		return
	}
	typNew, _ := rt.rdb.Type(rt.ctx, newKey).Result()
	if typNew != "none" {
		return
	}
	members, err := rt.rdb.SMembers(rt.ctx, oldKey).Result()
	if err != nil || len(members) == 0 {
		_ = rt.rdb.Del(rt.ctx, oldKey).Err()
		return
	}
	if err := rt.rdb.SAdd(rt.ctx, newKey, members).Err(); err != nil {
		rt.log.Warnf("migration: set copy failed queue=%s key=%s->%s err=%v", queue, oldKey, newKey, err)
		return
	}
	if err := rt.rdb.Del(rt.ctx, oldKey).Err(); err == nil {
		rt.log.Infof("migration: set moved queue=%s key=%s->%s count=%d", queue, oldKey, newKey, len(members))
	}
}
