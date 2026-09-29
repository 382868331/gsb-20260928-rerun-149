package clock

import (
	"sync"
	"time"
)

type SimClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewSimClock(start time.Time) *SimClock {
	return &SimClock{now: start.UTC()}
}

func (c *SimClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *SimClock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}
