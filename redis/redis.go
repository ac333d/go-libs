package redis

import (
	"fmt"
	"strconv"
	"time"

	redis "github.com/gomodule/redigo/redis"
)

// Session - Session
type Session = *redis.Pool

// Init - Connects to redis
func Init(host string, port int, password string, dbType int) (redis.Conn, error) {
	connTimeout := 20000 * time.Hour
	readTimeout := 2 * time.Second
	writeTimeout := 2 * time.Second

	sessionObj, err := redis.Dial("tcp", host+":"+strconv.Itoa(port),
		redis.DialConnectTimeout(connTimeout),
		redis.DialReadTimeout(readTimeout),
		redis.DialWriteTimeout(writeTimeout),
		redis.DialPassword(password),
		redis.DialDatabase(dbType),
	)

	if sessionObj == nil || err != nil {
		return nil, fmt.Errorf("Can not initialize redis client")
	}

	return sessionObj, nil
}

// InitPool - InitPool
func InitPool(host string, port int, password string, dbType int) (*redis.Pool, error) {
	pool := redis.Pool{
		MaxIdle:     5,
		MaxActive:   5,
		IdleTimeout: 240 * time.Second,
	}
	pool.Dial = func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", host+":"+strconv.Itoa(port))
		if err != nil {
			return nil, err
		}
		return c, err
	}
	pool.TestOnBorrow = func(c redis.Conn, t time.Time) error {
		if time.Since(t) < time.Minute {
			return nil
		}
		_, err := c.Do("PING")
		return err
	}

	conn := pool.Get()
	defer conn.Close()
	_, err := conn.Do("PING")
	if err != nil {
		return &pool, err
	}
	return &pool, nil
}

// Ping - Ping
func Ping(pool Session) error {
	conn := pool.Get()
	defer conn.Close()

	_, err := redis.String(conn.Do("PING"))
	if err != nil {
		return fmt.Errorf("cannot ping db: %v", err)
	}
	return nil
}

// Get - Get
func Get(pool Session, key string) ([]byte, error) {
	conn := pool.Get()
	defer conn.Close()

	var data []byte
	data, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return data, fmt.Errorf("error getting key %s: %v", key, err)
	}
	return data, nil
}

// Set - Set
func Set(pool Session, key string, value []byte) error {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, value)
	if err != nil {
		return fmt.Errorf("error setting key %s to [% x]: %v", key, value, err)
	}
	return nil
}

// HGet - HGet
func HGet(pool Session, key, field string) (string, error) {
	conn := pool.Get()
	defer conn.Close()

	result, err := redis.String(conn.Do("HGET", key, field))
	if err != nil {
		return "", fmt.Errorf("error getting key %s: %v", key, err)
	}

	return result, nil
}

// HSet - HSet
func HSet(pool Session, key, field, value string) error {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("HSET", key, field, value)
	if err != nil {
		return fmt.Errorf("error setting key %s to hash field %s with value %s: %v", key, field, value, err)
	}
	return nil
}

// HGetAll - HGetAll
func HGetAll(pool Session, key string) (map[string]string, error) {
	conn := pool.Get()
	defer conn.Close()

	data := make(map[string]string)
	result, err := redis.Strings(conn.Do("HGETALL", key))
	if err != nil {
		return data, fmt.Errorf("error getting key %s: %v", key, err)
	}

	for i := 0; i < len(result); i += 2 {
		key, _ := redis.String(result[i], nil)
		value, _ := redis.String(result[i+1], nil)
		data[key] = value
	}

	return data, nil
}

// HSetAll - HSetAll
func HSetAll(pool Session, key string, data map[string]string) error {
	conn := pool.Get()
	defer conn.Close()

	for k, v := range data {
		_, err := conn.Do("HSET", key, k, v)
		if err != nil {
			return fmt.Errorf("error setting key %s to hash field %s with value %s: %v", key, k, v, err)
		}
	}
	return nil
}

// HCacheAll - HCacheAll
func HCacheAll(pool Session, key string, value map[string]string, expiry int) error {
	if err := HSetAll(pool, key, value); err != nil {
		return err
	}

	return Expire(pool, key, expiry)
}

// HDel - HDel
func HDel(pool Session, key, field string) error {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key, field)
	if err != nil {
		return fmt.Errorf("error deleting the key %s: %v", key, err)
	}
	return nil
}

// GetString - GetString
func GetString(pool Session, key string) (string, error) {
	conn := pool.Get()
	defer conn.Close()

	data, err := redis.String(conn.Do("GET", key))
	if err != nil {
		return data, fmt.Errorf("error getting key %s: %v", key, err)
	}
	return data, nil
}

// GetStrings - GetStrings
func GetStrings(pool Session, key string) ([]string, error) {
	conn := pool.Get()
	defer conn.Close()

	data, err := redis.Strings(conn.Do("GET", key))
	if err != nil {
		return data, fmt.Errorf("error getting key %s: %v", key, err)
	}
	return data, nil
}

// SetString - SetString
func SetString(pool Session, key, value string) error {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, value)
	if err != nil {
		return fmt.Errorf("error setting key %s to %s: %v", key, value, err)
	}
	return nil
}

// Expire - Expire
func Expire(pool Session, key string, ttl int) error {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("EXPIRE", key, ttl)
	if err != nil {
		return fmt.Errorf("error setting expiry of key %s: %v", key, err)
	}
	return nil
}

// TTL - TTL
func TTL(pool Session, key string) (int, error) {
	conn := pool.Get()
	defer conn.Close()

	ttl, err := redis.Int(conn.Do("TTL", key))
	if err != nil {
		return ttl, fmt.Errorf("error getting ttl of key %s: %v", key, err)
	}
	return ttl, nil
}

// Exists - Exists
func Exists(pool Session, key string) (bool, error) {
	conn := pool.Get()
	defer conn.Close()

	ok, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return ok, fmt.Errorf("error checking if key %s exists: %v", key, err)
	}
	return ok, nil
}

// Delete - Delete
func Delete(pool Session, key string) error {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)
	if err != nil {
		return fmt.Errorf("error deleting the key %s: %v", key, err)
	}
	return nil
}

// GetKeys - GetKeys
func GetKeys(pool Session, pattern string) ([]string, error) {
	conn := pool.Get()
	defer conn.Close()

	iter := 0
	keys := []string{}
	for {
		arr, err := redis.Values(conn.Do("SCAN", iter, "MATCH", pattern))
		if err != nil {
			return keys, fmt.Errorf("error retrieving '%s' keys", pattern)
		}

		iter, _ = redis.Int(arr[0], nil)
		k, _ := redis.Strings(arr[1], nil)
		keys = append(keys, k...)

		if iter == 0 {
			break
		}
	}
	return keys, nil
}

// Incr - Incr
func Incr(pool Session, key string) (int, error) {
	conn := pool.Get()
	defer conn.Close()

	val, err := redis.Int(conn.Do("INCR", key))
	if err != nil {
		return 0, fmt.Errorf("error increasing the key %s: %v", key, err)
	}
	return val, nil
}

// Publish - Publish
func Publish(pool Session, key, val string) (int, error) {
	conn := pool.Get()
	defer conn.Close()

	num, err := redis.Int(conn.Do("PUBLISH", key, val))
	if err != nil {
		return 0, fmt.Errorf("error publishing the key %s: %v", key, err)
	}

	return num, nil
}

// LPush - LPush
func LPush(pool Session, key, value string) error {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("LPUSH", key, value)
	if err != nil {
		return fmt.Errorf("error setting list %s from left with value %s: %v", key, value, err)
	}
	return nil
}

// LPop - LPop
func LPop(pool Session, key string) (string, error) {
	conn := pool.Get()
	defer conn.Close()

	poppedElem, err := redis.String(conn.Do("LPOP", key))
	if err != nil {
		return poppedElem, fmt.Errorf("error popping list %s from left: %v", key, err)
	}
	return poppedElem, nil
}

// RPush - RPush
func RPush(pool Session, key, value string) error {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("RPUSH", key, value)
	if err != nil {
		return fmt.Errorf("error setting list %s from right with value %s: %v", key, value, err)
	}
	return nil
}

// RPop - RPop
func RPop(pool Session, key string) (string, error) {
	conn := pool.Get()
	defer conn.Close()

	poppedElem, err := redis.String(conn.Do("RPOP", key))
	if err != nil {
		return poppedElem, fmt.Errorf("error popping list %s from right: %v", key, err)
	}
	return poppedElem, nil
}

// LRange - LRange
func LRange(pool Session, key string, start, end int) ([]string, error) {
	conn := pool.Get()
	defer conn.Close()

	list, err := redis.Strings(conn.Do("LRANGE", key, start, end))
	if err != nil {
		return list, fmt.Errorf("error getting range (%d - %d) of list %s: %v", start, end, key, err)
	}
	return list, nil
}

// LLen - LLen
func LLen(pool Session, key string) (int, error) {
	conn := pool.Get()
	defer conn.Close()

	len, err := redis.Int(conn.Do("LLEN", key))
	if err != nil {
		return len, fmt.Errorf("error getting length of list %s: %v", key, err)
	}
	return len, nil
}
