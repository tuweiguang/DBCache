package DBCache

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/jinzhu/gorm"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"sync/atomic"
	"time"
)

const (
	notFoundPlaceholder = "*"

	defaultExpireTime         = 7200 * time.Second
	defaultNotFoundExpireTime = 3600 * time.Second
	defaultStatTick           = 60 * time.Second
)

var ErrPlaceholder = errors.New("placeholder")
var ErrNotFound = errors.New("not found")

type (
	// One table(or same multiple tables) one DBCache.
	DBCache struct {
		db  *gorm.DB
		rds *redis.Client //if rds id nil, default no cache
		log *zap.Logger

		name    string //table name
		barrier singleflight.Group

		expireTime         time.Duration //cache ttl
		notFoundExpireTime time.Duration //placeholder cache ttl

		statTick   time.Duration
		statTotal  uint64
		statHit    uint64
		statSwitch bool //default open
	}

	// custom attributes
	Option func(o *DBCache)

	// query function
	QueryFunc func(conn *gorm.DB, v interface{}) error

	// exec function
	ExecFunc func(conn *gorm.DB) error

	// hook function
	HookFunc func()

	// not found function
	NotFoundFunc func() (interface{}, error)
)

func NewCache(db *gorm.DB, redis *redis.Client, log *zap.Logger, name string, opts ...Option) *DBCache {
	if db == nil {
		panic("db is nil")
	}

	if log == nil {
		panic("log is nil")
	}

	cc := &DBCache{
		db:                 db,
		rds:                redis,
		log:                log,
		name:               name,
		expireTime:         defaultExpireTime,
		notFoundExpireTime: defaultNotFoundExpireTime,
		statSwitch:         true,
		statTick:           defaultStatTick,
	}

	for _, opt := range opts {
		opt(cc)
	}

	if cc.statSwitch {
		go cc.statLoop()
	}
	return cc
}

// WithExpireTime returns a func to customize a Options with given expireTime.
func WithExpireTime(expireTime time.Duration) Option {
	if expireTime <= 0 {
		expireTime = defaultExpireTime
	}
	return func(cc *DBCache) {
		cc.expireTime = expireTime
	}
}

// WithNotFoundExpireTime returns a func to customize a Options with given notFoundExpireTime.
func WithNotFoundExpireTime(notFoundExpireTime time.Duration) Option {
	if notFoundExpireTime <= 0 {
		notFoundExpireTime = defaultNotFoundExpireTime
	}
	return func(cc *DBCache) {
		cc.notFoundExpireTime = notFoundExpireTime
	}
}

// WithStatTick returns a func to customize a Options with given statTick.
func WithStatTick(statTick time.Duration) Option {
	if statTick <= 0 {
		statTick = defaultStatTick
	}
	return func(cc *DBCache) {
		cc.statTick = statTick
	}
}

// WithStatSwitch returns a func to customize a Options with given statSwitch.
func WithStatSwitch(open bool) Option {
	return func(cc *DBCache) {
		cc.statSwitch = open
	}
}

// Exec can insert or update or delete
func (c *DBCache) Exec(key string, exec ExecFunc) error {
	if c.rds == nil {
		return c.ExecNoCache(exec)
	}
	err := exec(c.db)
	if err != nil {
		return err
	}

	if _, err := c.rds.Del(key).Result(); err != nil {
		c.log.Error(fmt.Sprintf("DBCache.Exec delete cache fail. key: %v, error: %v", key, err))
		//todo 异步延迟删除
		return err
	}
	return nil
}

func (c *DBCache) SetCache(key string, v interface{}) error {
	if c.rds == nil || v == nil {
		return nil
	}
	return c.rds.Set(key, v, c.expireTime).Err()
}

func (c *DBCache) ExecNoCache(exec ExecFunc) error {
	err := exec(c.db)
	if err != nil {
		return err
	}

	return nil
}

// QueryRow can select
// if notFoundFunc is nil,and query return is not found,default use setCacheWithNotFound.
func (c *DBCache) QueryRow(v interface{}, key string, query QueryFunc, notFoundFunc NotFoundFunc) error {
	if c.rds == nil {
		return c.QueryRowNoCache(v, query)
	}

	return c.take(v, key, func(vv interface{}) error {
		return query(c.db, vv)
	}, func(vv interface{}) ([]byte, error) {
		data, err := json.Marshal(vv)
		if err != nil {
			return nil, err
		}
		return data, c.rds.Set(key, data, c.expireTime).Err()
	}, notFoundFunc)
}

func (c *DBCache) QueryRowNoCache(v interface{}, query QueryFunc) error {
	return query(c.db, v)
}

func (c *DBCache) take(v interface{}, key string, query func(v interface{}) error,
	cacheVal func(v interface{}) ([]byte, error), notFoundFunc NotFoundFunc) error {
	val, err, shared := c.barrier.Do(key, func() (interface{}, error) {
		var data []byte
		var err error
		if data, err = c.getCache(key, v); err != nil {
			if err == ErrPlaceholder {
				return nil, ErrNotFound
			} else if err != ErrNotFound {
				// other error, return.
				return nil, err
			}

			if err = query(v); err == gorm.ErrRecordNotFound {
				if notFoundFunc != nil {
					// custom not found function
					return notFoundFunc()
				} else {
					if err = c.setCacheWithNotFound(key); err != nil {
						c.log.Error(err.Error())
					}
				}

				return nil, ErrNotFound
			} else if err != nil {
				return nil, err
			}

			if data, err = cacheVal(v); err != nil {
				c.log.Error(err.Error())
			} else {
				return data, nil
			}
		}

		return data, err
	})
	if err != nil {
		return err
	}
	if !shared {
		return nil
	}

	return json.Unmarshal(val.([]byte), v)
}

func (c *DBCache) getCache(key string, v interface{}) ([]byte, error) {
	atomic.AddUint64(&c.statTotal, 1)
	data, err := c.rds.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if len(data) == 0 {
		return nil, ErrNotFound
	}

	atomic.AddUint64(&c.statHit, 1)
	if data == notFoundPlaceholder {
		return nil, ErrPlaceholder
	}

	return c.processCache(key, data, v)
}

func (c *DBCache) processCache(key, data string, v interface{}) ([]byte, error) {
	d := []byte(data)
	err := json.Unmarshal(d, v)
	if err == nil {
		return d, nil
	}

	if _, e := c.rds.Del(key).Result(); e != nil {
		c.log.Error(fmt.Sprintf("DBCache.processCache delete cache. key: %s, value: %s, error: %v", key, data, e))
	}

	return nil, ErrNotFound
}

func (c *DBCache) setCacheWithNotFound(key string) error {
	return c.rds.Set(key, notFoundPlaceholder, c.notFoundExpireTime).Err()
}

func (c *DBCache) statLoop() {
	t := time.NewTicker(c.statTick)
	defer t.Stop()

	for range t.C {
		total := atomic.SwapUint64(&c.statTotal, 0)
		if total == 0 {
			continue
		}

		hit := atomic.SwapUint64(&c.statHit, 0)
		percent := 100 * float32(hit) / float32(total)
		miss := total - hit
		c.log.Info(fmt.Sprintf("DBCache.statLoop(%s) - qpm: %d, hit_ratio: %.1f, hit: %d, miss: %d", c.name, total, percent, hit, miss))
	}
}
