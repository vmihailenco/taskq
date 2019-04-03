/*
Package taskq implements task/job queue with in-memory, SQS, IronMQ backends.

taskq is a thin wrapper for SQS and IronMQ clients that uses Redis to implement rate limiting and call once semantic.

taskq consists of following components:
 - memqueue - in memory queue that can be used for local unit testing.
 - azsqs - Amazon SQS backend.
 - ironmq - IronMQ backend.
 - Factory - provides common interface for creating new queues.
 - Consumer - queue processor that works with memqueue, azsqs, and ironmq.

rate limiting is implemented in the processor package using https://github.com/go-redis/redis_rate. Call once is implemented in clients by checking if message name exists in Redis database.
*/
package taskq
