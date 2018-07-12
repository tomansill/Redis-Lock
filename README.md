# Redis Distributed Lock

Work in progress

## Design and Goals
The goal of this project to implement distributed locking mechanism using Redis server (single server or multiple nodes in the cluster) using the (RedLock)[https://redis.io/topics/distlock] algorithm. The project looks to implement fair and non-fair Java concurrent `ReadWriteLock` mechanisms. The project aims to be loosely coupled and not depend on specific Java Redis library to operate meaning any Redis client such as Jedis, Lettuce, RedisClient, Redisson, etc. will work with this library as long a wrapper class that implements `RedisClient` interface is used to wrap the library.
