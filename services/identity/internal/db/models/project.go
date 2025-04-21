package models

import "github.com/google/uuid"

type Project struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   string    `json:"created_at"`
	OwnerID     uuid.UUID `json:"owner_id"`
}
