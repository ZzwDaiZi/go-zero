package redis

import (
	"math/rand"
	"sync/atomic"
	"time"

	red "github.com/go-redis/redis"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stringx"
)

const (
	//KEYS[1]: 锁key
	//ARGV[1]: 锁value,随机字符串
	//--释放锁
	//--不可以释放别人的锁
	delCommand = `if redis.call("GET", KEYS[1]) == ARGV[1] then
	--执行成功返回"1"
    return redis.call("DEL", KEYS[1])
else
    return 0
end`
	randomLen = 16
)

// A RedisLock is a redis lock.
// redis 分布式锁
type RedisLock struct {
	// Redis 存储
	store   *Redis
	// 超时时间
	seconds uint32
	count   int32
	// 锁key
	key     string
	// 锁value，防止锁被别人获取到
	id      string
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// NewRedisLock returns a RedisLock.
// 初始化返回 redis分布式锁
func NewRedisLock(store *Redis, key string) *RedisLock {
	return &RedisLock{
		store: store,
		key:   key,
		//获取锁时，锁的值通过随机字符串生成
		//实际上go-zero提供更加高效的随机字符串生成方式
		id:    stringx.Randn(randomLen),
	}
}

// Acquire acquires the lock.
// 加锁,不可重入锁
func (rl *RedisLock) Acquire() (bool, error) {
	// 防止重入锁
	newCount := atomic.AddInt32(&rl.count, 1)
	if newCount > 1 {
		return true, nil
	}

	// 获取过期时间
	seconds := atomic.LoadUint32(&rl.seconds)
	// 加锁，默认过期时间1s
	ok, err := rl.store.SetnxEx(rl.key, rl.id, int(seconds+1)) // +1s for tolerance
	if err == red.Nil {
		atomic.AddInt32(&rl.count, -1)
		return false, nil
	} else if err != nil {
		atomic.AddInt32(&rl.count, -1)
		logx.Errorf("Error on acquiring lock for %s, %s", rl.key, err.Error())
		return false, err
	} else if !ok {
		atomic.AddInt32(&rl.count, -1)
		return false, nil
	}

	return true, nil
}

// Release releases the lock.
// 释放锁
func (rl *RedisLock) Release() (bool, error) {
	newCount := atomic.AddInt32(&rl.count, -1)
	if newCount > 0 {
		return true, nil
	}

	resp, err := rl.store.Eval(delCommand, []string{rl.key}, []string{rl.id})
	if err != nil {
		return false, err
	}

	reply, ok := resp.(int64)
	if !ok {
		return false, nil
	}

	return reply == 1, nil
}

// SetExpire sets the expiration.
//需要注意的是需要在Acquire()之前调用,调用之后是累加过期时间 1s + seconds
//不然默认为1s自动释放
func (rl *RedisLock) SetExpire(seconds int) {
	atomic.StoreUint32(&rl.seconds, uint32(seconds))
}
