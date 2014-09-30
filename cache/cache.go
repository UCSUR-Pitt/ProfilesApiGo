// Redis caching for Profiles API
package cache

import (
    "crypto/md5"
    "fmt"
	"io"
    "github.com/garyburd/redigo/redis"
    "time"
)

// Connect to Redis return a redis.Conn
// Make sure to close this connection
func RedisConn(conn_string string)(redis.Conn, error) {
    c, err := redis.DialTimeout("tcp", conn_string , 0, 1*time.Second, 1*time.Second)
    return c, err
}

// Set a key in redis return error status
func SetKey(conn redis.Conn, key string, value []byte, expire int) error {
    _ , err := conn.Do("SETEX", key, expire, value)
    return err
}

func GetKey(conn redis.Conn, key string) ([]byte, error) {
    res, err := conn.Do("GET", key)
    if err != nil {
        return nil, err
    }
    if res == nil{
        var res []byte
        return res, err
    }
    return res.([]byte), err
}


// Create a hash from a string using md5
func MakeHash(s string) string {
    h := md5.New()
	io.WriteString(h, s)
	return fmt.Sprintf("%x", h.Sum(nil))
}



