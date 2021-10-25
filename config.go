package mcache

import "time"

const (
	_DefaultMinCacheSize     = 8 * 1024 * 1024
	_DefaultMaxCacheSize     = 4096 * 1024 * 1024
	_DefaultMinCacheGroups   = 3
	_DefaultMaxCacheGroups   = 9
	_DefaultFlushMultipleOps = 1000000
	_DefaultFlushPeriodTime  = time.Second
)

type config struct {
	cacheFileSize    uint64
	cacheGroup       int
	flushPeriodTime  time.Duration
	flushMultipleOps int
}

type option func(*config) error

func newConfig() *config {
	return &config{
		cacheFileSize:    _DefaultMinCacheSize,
		cacheGroup:       _DefaultMinCacheGroups,
		flushPeriodTime:  _DefaultFlushPeriodTime,
		flushMultipleOps: _DefaultFlushMultipleOps,
	}
}

func SetCacheSize(cacheSize uint64) option {
	return func(c *config) error {
		if cacheSize > _DefaultMinCacheSize && cacheSize <= _DefaultMaxCacheSize {
			c.cacheFileSize = cacheSize
		}
		return nil
	}
}

func SetCacheGroups(cacheGroup int) option {
	return func(c *config) error {
		if cacheGroup > _DefaultMinCacheGroups && cacheGroup <= _DefaultMaxCacheGroups {
			c.cacheGroup = cacheGroup
		}
		return nil
	}
}

func SetPeriodicFlushDuration(flushPeriod time.Duration) option {
	return func(c *config) error {
		c.flushPeriodTime = flushPeriod
		return nil
	}
}

func SetPeriodicFlushMultipleOps(flushMultipleOps int) option {
	return func(c *config) error {
		c.flushMultipleOps = flushMultipleOps
		return nil
	}
}
