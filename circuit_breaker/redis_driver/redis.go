package redisdriver

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

const (
	requests             = "requests"
	totalSuccesses       = "total_successes"
	totalFailures        = "total_failures"
	consecutiveSuccesses = "consecutive_successes"
	consecutiveFailures  = "consecutive_failures"
)

type Counts struct {
	redisClient redis.UniversalClient
	key         string
}

func New(rds redis.UniversalClient, key string) *Counts {
	return &Counts{
		redisClient: rds,
		key:         key,
	}
}

func (c *Counts) OnRequest(ctx context.Context) error {
	if err := c.redisClient.HIncrBy(ctx, c.key, requests, 1).Err(); err != nil {
		return err
	}

	return nil
}

func (c *Counts) OnSuccess(ctx context.Context) error {
	if err := c.redisClient.HIncrBy(ctx, c.key, totalSuccesses, 1).Err(); err != nil {
		return err
	}

	if err := c.redisClient.HIncrBy(ctx, c.key, consecutiveSuccesses, 1).Err(); err != nil {
		return err
	}

	if err := c.redisClient.HMSet(ctx, c.key, consecutiveFailures, 0).Err(); err != nil {
		return err
	}

	return nil
}

func (c *Counts) OnFailure(ctx context.Context) error {
	if err := c.redisClient.HIncrBy(ctx, c.key, totalFailures, 1).Err(); err != nil {
		return err
	}

	if err := c.redisClient.HIncrBy(ctx, c.key, consecutiveFailures, 1).Err(); err != nil {
		return err
	}

	if err := c.redisClient.HMSet(ctx, c.key, consecutiveSuccesses, 0).Err(); err != nil {
		return err
	}

	return nil
}

func (c *Counts) Clear(ctx context.Context) error {
	return c.redisClient.Del(ctx, c.key).Err()
}

func (c *Counts) GetField(ctx context.Context, field string) (uint32, error) {
	result, err := c.redisClient.HGet(ctx, c.key, field).Result()
	if err != nil {
		return 0, err
	}

	var value uint32
	if err := json.Unmarshal([]byte(result), &value); err != nil {
		return 0, err
	}

	return value, nil
}
