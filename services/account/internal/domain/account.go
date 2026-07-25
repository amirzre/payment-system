package domain

import (
	"time"

	"github.com/google/uuid"
)

// Status constants for account lifecycle.
const (
	StatusActive = "active"
	StatusFrozen = "frozen"
	StatusClosed = "closed"
)

// Account represents a payment account owned by a user.
type Account struct {
	ID        uuid.UUID
	OwnerRef  string
	Currency  string
	Status    string
	CreatedAt time.Time
}
