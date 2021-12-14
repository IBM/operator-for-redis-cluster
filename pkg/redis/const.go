package redis

import "time"

// Redis admin constants
const (
	// HashMaxSlots numbers or redis slots used for key hashing
	// as slots start at 0, total number of slots is HashMaxSlots+1
	HashMaxSlots = 16383
	// ResetHard HARD mode for RESET command
	ResetHard = "HARD"
	// ResetSoft SOFT mode for RESET command
	ResetSoft = "SOFT"
)

// Redis client constants
const (
	defaultClientName    = ""
	defaultClientTimeout = 2 * time.Second
	defaultRetryTimeout  = 3 * time.Second
	defaultRetryAttempts = 3
)

// Redis error constants
const (
	// ErrNotFound cannot find a node to connect to
	ErrNotFound = "Unable to find a node to connect"
)
