package bloom

import (
	"errors"
	"strconv"

	"github.com/zeromicro/go-zero/core/hash"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

const (
	// for detailed error rate table, see http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
	// maps as k in the error rate table
	// 14次散列函数计算
	maps      = 14

	// lua 脚本
	// 为什么一定要用lua脚本呢? 因为需要保证整个操作是原子性执行的。

	//ARGV:偏移量offset数组
	//KYES[1]: setbit操作的key
	//全部设置为1
	setScript = `
for _, offset in ipairs(ARGV) do
	redis.call("setbit", KEYS[1], offset, 1)
end
`
	//ARGV:偏移量offset数组
	//KYES[1]: setbit操作的key
	//检查是否全部为1
	testScript = `
for _, offset in ipairs(ARGV) do
	if tonumber(redis.call("getbit", KEYS[1], offset)) == 0 then
		return false
	end
end
return true
`
)

// ErrTooLargeOffset indicates the offset is too large in bitset.
var ErrTooLargeOffset = errors.New("too large offset")

type (
	// A Filter is a bloom filter.
	// 布隆过滤器结构体
	Filter struct {
		bits   uint
		bitSet bitSetProvider
	}

	// 位数组操作接口定义
	bitSetProvider interface {
		check([]uint) (bool, error)
		set([]uint) error
	}
)

// New create a Filter, store is the backed redis, key is the key for the bloom filter,
// 新建一个Filter，store是redis，key是bloom filter的key。
// bits is how many bits will be used, maps is how many hashes for each addition.
// bits是将使用的比特数，maps是每个加法的散列数。
// best practices:
// 最佳实践:
// elements - means how many actual elements
// elements--意味着有多少个实际的元素
// when maps = 14, formula: 0.7*(bits/maps), bits = 20*elements, the error rate is 0.000067 < 1e-4
// 当maps=14时，公式:0.7*（bits/maps），bits=20*elements，错误率为0.000067 < 1e-4
// for detailed error rate table, see http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
func New(store *redis.Redis, key string, bits uint) *Filter {
	return &Filter{
		bits:   bits,
		bitSet: newRedisBitSet(store, key, bits),
	}
}

// Add adds data into f.
// ADD 添加一个元素到布隆过滤器
func (f *Filter) Add(data []byte) error {
	locations := f.getLocations(data)
	return f.bitSet.set(locations)
}

// Exists checks if data is in f.
// Exists 检查元素是否在布隆过滤器中
func (f *Filter) Exists(data []byte) (bool, error) {
	locations := f.getLocations(data)
	isSet, err := f.bitSet.check(locations)
	if err != nil {
		return false, err
	}
	if !isSet {
		return false, nil
	}

	return true, nil
}

// k次散列计算出k个offset  k=maps
func (f *Filter) getLocations(data []byte) []uint {
	//创建指定容量的切片
	locations := make([]uint, maps)
	//maps表示k值,作者定义为了常量:14
	for i := uint(0); i < maps; i++ {
		//哈希计算,使用的是"MurmurHash3"算法,并每次追加一个固定的i字节进行计算
		hashValue := hash.Hash(append(data, byte(i)))
		//取下标offset
		locations[i] = uint(hashValue % uint64(f.bits))
	}

	return locations
}

// redis 位数组
type redisBitSet struct {
	store *redis.Redis
	key   string
	bits  uint
}

// 初始化redis位数组
func newRedisBitSet(store *redis.Redis, key string, bits uint) *redisBitSet {
	return &redisBitSet{
		store: store,
		key:   key,
		bits:  bits,
	}
}

// 检查偏移量数组是否合法，并将类型uint 转为 string
func (r *redisBitSet) buildOffsetArgs(offsets []uint) ([]string, error) {
	var args []string

	for _, offset := range offsets {
		if offset >= r.bits {
			return nil, ErrTooLargeOffset
		}

		args = append(args, strconv.FormatUint(uint64(offset), 10))
	}

	return args, nil
}

// check 检查偏移量offset数组是否全部为1
// offsets 散列函数后得出得偏移量数组
//是:元素可能存在
//否:元素一定不存在
func (r *redisBitSet) check(offsets []uint) (bool, error) {
	args, err := r.buildOffsetArgs(offsets)
	if err != nil {
		return false, err
	}

	// 执行lua脚本
	resp, err := r.store.Eval(testScript, []string{r.key}, args)
	//底层使用的go-redis
	//redis.Nil表示key不存在的情况需特殊判断
	if err == redis.Nil {
		return false, nil
	} else if err != nil {
		return false, err
	}

	exists, ok := resp.(int64)
	if !ok {
		return false, nil
	}

	return exists == 1, nil
}

// 删除位数组
func (r *redisBitSet) del() error {
	_, err := r.store.Del(r.key)
	return err
}

// 设置过期时间
func (r *redisBitSet) expire(seconds int) error {
	return r.store.Expire(r.key, seconds)
}

// 将偏移量的位置全部设置为1
func (r *redisBitSet) set(offsets []uint) error {
	args, err := r.buildOffsetArgs(offsets)
	if err != nil {
		return err
	}

	_, err = r.store.Eval(setScript, []string{r.key}, args)
	if err == redis.Nil {
		return nil
	}

	return err
}
