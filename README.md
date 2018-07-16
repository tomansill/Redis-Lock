# Redis Distributed Lock

## Project Status
This library is a **work in progress** project. As of now, we have no release plans. Our design, goals, and roadmap are just rough estimates and nowhere near factual. Anything on this project is not permanent and may be changed any time.

## Design and Goals
The goal of this project to provide distributed locking mechanisms in Java using a Redis database (single server or multiple nodes in the cluster) using the [RedLock](https://redis.io/topics/distlock) algorithm. The project looks to implement fair and non-fair Java [ReadWriteLock](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/ReadWriteLock.html) pattern.

The project aims to be loosely coupled and independent of specific Java Redis library to operate. This enables the library to work with any Redis Java clients such as [Jedis](https://github.com/xetorthio/jedis), [Lettuce](https://github.com/lettuce-io/lettuce-core), [Jedipus](https://github.com/jamespedwards42/jedipus), etc. as long a wrapper class that implements `AbstractRedisClient` interface is used to wrap the Redis library.

## Status

 - API Design (Work-In-Progress)
 - Single Redis Server Locking (Work-In-Progress)
	 - Unfair ReadLock (Work-In-Progress)
	 - Unfair WriteLock (Work-In-Progress)
	 - Fair ReadLock(Work-In-Progress)
	 - Fair WriteLock (Work-In-Progress)
	 - Condition (Not Started)
 - Redis Cluster Locking (Not Started)
	 - Unfair ReadLock (Not Started)
	 - Unfair WriteLock (Not Started)
	 - Fair ReadLock (Not Started)
	 - Fair WriteLock (Not Started)
	 - Condition (Not Started)
 - Clients (Not Started)
 	 **NOTE:** The implemented clients will be available as a separate and dependent package in the future.
	 - [Jedis](https://github.com/xetorthio/jedis) (Not Started)
	 - [Lettuce](https://github.com/lettuce-io/lettuce-core) (Not Started)
 - Testing (Not Started)
	 - Multiple WriteLocks (Not Started)
	 - Multiple ReadLocks (Not Started)
	 - Multiple ReadLocks after single WriteLock (Not Started)
	 - Multiple ReadLocks before single WriteLock (Not Started)
	 - Zippered ReadLocks and WriteLocks (Not Started)
     - Client failovers (Not Started)
     - Cluster failovers (Not Started)
