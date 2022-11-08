# DBCache
在服务器开发中,在高并发场景对db频繁读取,若不加cache,会对db造成很大的压力,所以一般解决办法就是加一层cache进行加速.
但是在日常开发中发现,开发人员每次创建新的db table都要重新手写cache代码,而这块逻辑都是重复的,所以DBCache很好的解决这类问题.
DBCache是对db数据进行自动缓存的工具,无需开发人员手动添加缓存代码,减少重复逻辑,提高效率.
目前只支持`github.com/jinzhu/gorm`作为db,`github.com/go-redis/redis`作为cache和`go.uber.org/zap`作为log.

# Design
- 在缓存一致性中,DBCache采用的模式是:
  - 读取操作
      1. 先读取cache有数据返回.
      2. 若无数据,读取db,设置到cache中.
  - 执行操作
      1. 先执行操作db,删除cache.
      
  
  对于大多数场景是能够满足的,但是该模式也会存在一些极端问题,比如请求1先执行操作db后还未来得及删除cache,请求2读取cache得到的是老数据,但是最终保证数据的一致性.对于缓存一致性各模式这里不讨论.
- 只需提供key,DBCache就能自动从`QueryFunc`读取的数据设置到对应key的cache中.
- 使用`singleflight.Group`对同key防止内存击穿.
- 若读取db not found,默认会设置占位符到cache,防止内存穿透.当然可以自定义NotFoundFunc函数设置cache.
- 若不确定是否使用cache,可以开启stat数据采集(默认开启).
- 若cache命中率不高,不需要cache,在初始化DBCache是只需将cache设置为nil.

# How to use
初始化DBCache:
```go
userCache := cache.NewCache(db, redis, log, "users")
```
查询:
```go
u := &User{}
err := userCache.QueryRow(u, key, func(conn *gorm.DB, v interface{}) error {  
    result := conn.Debug().Raw("SELECT * FROM `users`  WHERE (username = ? AND secret = ?) "+
  		"ORDER BY `users`.`id` ASC LIMIT 1", user.UserName, user.Secret).Scan(v)
  	return result.Error
}, nil)
```
执行:
```go
key := fmt.Sprintf("users:%v", user.UserName)
err := dc.Exec(key, func(conn *gorm.DB) error {
	result := conn.Debug().Create(user)
	return result.Error
})
```

# Test & Benchmark
```
goos: windows
goarch: amd64
cpu: Intel(R) Core(TM) i5-8500 CPU @ 3.00GHz
BenchmarkDBCache_QueryRow_DifferKey-6                       5673            218400 ns/op           12883 B/op        235 allocs/op
BenchmarkDBCache_OriginalQueryRow_DifferKey-6               6810            178642 ns/op           12670 B/op        216 allocs/op
BenchmarkDBCache_QueryRow_SameKey-6                        27079             43116 ns/op            1997 B/op         50 allocs/op
BenchmarkDBCache_OriginalQueryRow_SameKey-6                 7021            178258 ns/op           11895 B/op        213 allocs/op
PASS
```
可以看出相同key,在高并发情况下DBCache表现更优异;但是在不同key的高并发下,稍微逊色.所以DBCache适合读多写少场景.

# Todo list
1. 强依赖`gorm.DB`,`redis.Client`和`zap.Logger`,能否抽象出来.
2. 引入延时队列,对删除cache失败进行尝试延时删除.
3. 添加cache随机ttl,防止缓存雪崩.



# Reference

[go-zero](https://github.com/zeromicro/go-zero)