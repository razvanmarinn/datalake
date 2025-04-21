package models

import "github.com/google/uuid"

type User interface {
	GetID() int
}

type Client struct {
	ID        uuid.UUID `json:"id"`
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	Password  string    `json:"password"`
	CreatedAt string    `json:"created_at"`
}
