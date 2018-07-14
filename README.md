# Redis Distributed Lock

## Project Status
This library is a **work in progress** project. Currently, we have no release plans. Our design, goals, and roadmap are just rough estimates and anywhere near factual. Anything on this project is not permanent and may be changed any time.

## Design and Goals
The goal of this project to implement distributed locking mechanism in Redis server (single server or multiple nodes in the cluster) using the [RedLock](https://redis.io/topics/distlock) algorithm. The project looks to implement fair and non-fair Java [ReadWriteLock](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/ReadWriteLock.html) mechanism.

The project aims to be loosely coupled and independent of specific Java Redis library to operate. This enables the library to work with any Redis clients such as [Jedis](https://github.com/xetorthio/jedis), [Lettuce](https://github.com/lettuce-io/lettuce-core), [Jedipus](https://github.com/jamespedwards42/jedipus), etc. as long a wrapper class that implements `RedisClient` interface is used to wrap the Redis library.

## Status

 - API Design (Work-In-Progress)
 - Single Node Locking (Work-In-Progress)
	 - Unfair ReadLock (Work-In-Progress)
	 - Unfair WriteLock (Work-In-Progress)
	 - Fair ReadLock(Work-In-Progress)
	 - Fair WriteLock (Work-In-Progress)
	 - Condition (Not Started)
 - Cluster Locking (Not Started)
	 - Fair ReadLock (Not Started)
	 - Fair WriteLock (Not Started)
	 - Unfair ReadLock (Not Started)
	 - Unfair WriteLock (Not Started)
	 - Condition (Not Started)
 - Clients
	 - LocalTestClient (Not Started)
	 - [Jedis](https://github.com/xetorthio/jedis) (Not Started)
	 - [Lettuce](https://github.com/lettuce-io/lettuce-core) (Not Started)
 - Testing
	 - Multiple WriteLocks
	 - Multiple ReadLocks
	 - Multiple ReadLocks after single WriteLock
	 - Multiple ReadLocks before single WriteLock
	 - Zippered ReadLocks and WriteLocks
